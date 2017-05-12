package com.criteo.cuttle

import java.time.{LocalDateTime, ZonedDateTime}
import java.time.format.DateTimeFormatter.{ISO_INSTANT}

import scala.util.{Success,Failure}
import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.stm._
import scala.concurrent.stm.Txn.ExternalDecider
import scala.reflect.{classTag, ClassTag}

import lol.http.{ PartialService }

import doobie.imports._

import io.circe._

sealed trait ExecutionStatus
case object ExecutionSuccessful extends ExecutionStatus
case object ExecutionFailed extends ExecutionStatus
case object ExecutionRunning extends ExecutionStatus
case object ExecutionPaused extends ExecutionStatus

case class ExecutionLog(
  id: String,
  job: String,
  startTime: LocalDateTime,
  endTime: Option[LocalDateTime],
  context: Json,
  status: ExecutionStatus
)

trait ExecutionStreams {
  def info(str: CharSequence) = this.println("INFO ", str)
  def error(str: CharSequence) = this.println("ERROR", str)
  def debug(str: CharSequence) = this.println("DEBUG", str)
  private def println(tag: String, str: CharSequence): Unit = {
    val time = ZonedDateTime.now().format(ISO_INSTANT)
    str.toString.split("\n").foreach(l => this.println(s"$time $tag - $l"))
  }
  def println(str: CharSequence): Unit
}

object ExecutionCancelled extends RuntimeException("Execution cancelled")

case class Execution[S <: Scheduling](
  id: String,
  job: Job[S],
  context: S#Context,
  startTime: LocalDateTime,
  streams: ExecutionStreams,
  platforms: Seq[ExecutionPlatform[S]]
) {
  private[cuttle] val cancelSignal = Promise[Unit]
  def isCancelled = cancelSignal.isCompleted
  val cancelled = cancelSignal.future
  def onCancelled(thunk: () => Unit) = cancelled.andThen {
    case Success(_) =>
      thunk()
    case Failure(_) =>
  }
  def cancel(): Boolean = {
    if(cancelSignal.trySuccess(())) {
      streams.debug(s"Execution has been cancelled.")
      true
    }
    else false
  }

  private[cuttle] def toExecutionLog(status: ExecutionStatus) =
    ExecutionLog(
      id,
      job.id,
      startTime,
      None,
      context.toJson,
      status
    )
}

case class SubmittedExecution[S <: Scheduling](execution: Execution[S], result: Future[Unit])

trait ExecutionPlatform[S <: Scheduling] {
  def routes: PartialService = PartialFunction.empty
}

object ExecutionPlatform {
  implicit def fromExecution[S <: Scheduling](implicit e: Execution[S]): Seq[ExecutionPlatform[S]] = e.platforms
  def lookup[E: ClassTag](implicit platforms: Seq[ExecutionPlatform[_]]): Option[E] =
    platforms.find(classTag[E].runtimeClass.isInstance).map(_.asInstanceOf[E])
}

case class Executor[S <: Scheduling](platforms: Seq[ExecutionPlatform[S]], queries: Queries, xa: XA)(implicit contextOrdering: Ordering[S#Context]) {

  private val pausedState = {
    val byId = TMap.empty[String, Map[Execution[S], Promise[Unit]]]
    val pausedIds = queries.getPausedJobIds.transact(xa).unsafePerformIO
    byId.single ++= pausedIds.map((_, Map.empty[Execution[S], Promise[Unit]]))
    byId
  }

  private val runningState = TMap.empty[Execution[S], Future[Unit]]

  def runningExecutions: Seq[ExecutionLog] =
    runningState.single.snapshot.keys.toSeq.map { execution =>
      execution.toExecutionLog(ExecutionRunning)
    }

  def pausedExecutions: Seq[ExecutionLog] =
    pausedState.single.values.flatMap(_.keys).toSeq.map { execution =>
      execution.toExecutionLog(ExecutionPaused)
    }

  def pausedJobs: Seq[String] =
    pausedState.single.keys.toSeq

  def archivedExecutions: Seq[ExecutionLog] =
    queries.getExecutionLog.transact(xa).unsafePerformIO

  def cancelExecution(executionId: String): Unit = {
    (runningState.single.keys ++ pausedState.single.values.flatMap(_.keys)).
      find(_.id == executionId).foreach(_.cancel())
  }

  def unpauseJob(job: Job[S]): Unit = {
    val executionsToResume = atomic { implicit tx =>
      Txn.setExternalDecider(new ExternalDecider {
        def shouldCommit(implicit txn: InTxnEnd): Boolean = {
          queries.unpauseJob(job).transact(xa).unsafePerformIO
          true
        }
      })
      val executions = pausedState.get(job.id)
      pausedState -= job.id
      executions.map { executions =>
        executions.foreach { case (execution, promise) =>
          runningState += (execution -> promise.future)
        }
        executions
      }.getOrElse(Nil)
    }
    executionsToResume.toList.sortBy(_._1.context).foreach { case (execution, promise) =>
      execution.streams.debug(s"Job ${job.id} has been unpaused.")
      unsafeDoRun(execution, promise)
    }
  }

  def pauseJob(job: Job[S]): Unit = {
    val executionsToCancel = atomic { implicit tx =>
      Txn.setExternalDecider(new ExternalDecider {
        def shouldCommit(implicit txn: InTxnEnd): Boolean = {
          queries.pauseJob(job).transact(xa).unsafePerformIO
          true
        }
      })
      pausedState += (job.id -> Map.empty)
      runningState.filterKeys(_.job == job).keys
    }
    executionsToCancel.toList.sortBy(_.context).reverse.foreach { execution =>
      execution.streams.debug(s"Job ${job.id} has been paused.")
      execution.cancel()
    }
  }

  private def unsafeDoRun(execution: Execution[S], promise: Promise[Unit]): Unit = {
    execution.streams.debug(s"Starting execution.")
    promise.completeWith(execution.job.effect(execution).
      andThen {
        case Success(()) =>
          execution.streams.debug(s"Execution successful.")
        case Failure(e) =>
          execution.streams.debug(s"Execution failed with message: ${e.getMessage}.")
      }.
      andThen {
        case result =>
          atomic { implicit tx => runningState -= execution }
          queries
            .logExecution(
              execution.
                toExecutionLog(if(result.isSuccess) ExecutionSuccessful else ExecutionFailed).
                copy(endTime = Some(LocalDateTime.now()))
            )
            .transact(xa)
            .unsafePerformIO
      })
  }

  def runAll(all: Seq[(Job[S], S#Context)]): Seq[SubmittedExecution[S]] = all.sortBy(_._2).map((run _).tupled)

  def run(job: Job[S], context: S#Context): SubmittedExecution[S] = {
    val existingOrNew: Either[SubmittedExecution[S],(Execution[S], Promise[Unit], Boolean)] = atomic { implicit tx =>
      val maybeAlreadyRunning: Option[(Execution[S], Future[Unit])] =
        runningState.find { case (e, f) => e.job == job && e.context == context }

      lazy val maybePaused: Option[(Execution[S], Future[Unit])] =
        pausedState.
          get(job.id).
          getOrElse(Map.empty).
          find { case (e, p) => e.job == job && e.context == context }.
          map { case (e, p) => (e, p.future) }

      maybeAlreadyRunning.orElse(maybePaused).map((SubmittedExecution.apply[S] _).tupled).toLeft {
        val nextExecutionId = utils.randomUUID
        val execution = Execution(
          id = nextExecutionId,
          job = job,
          context = context,
          startTime = LocalDateTime.now(),
          streams = new ExecutionStreams {
            def println(str: CharSequence) = System.out.println(s"@$nextExecutionId - $str")
          },
          platforms = platforms
        )
        val promise = Promise[Unit]

        if(pausedState.contains(job.id)) {
          val pausedExecutions = pausedState(job.id) + (execution -> promise)
          pausedState += (job.id -> pausedExecutions)
          (execution, promise, false)
        }
        else {
          runningState += (execution -> promise.future)
          (execution, promise, true)
        }
      }
    }

    existingOrNew.fold(identity, { case (execution, promise, doRunNow) =>
      execution.streams.debug(s"Execution ${execution.id}, for ${execution.context}")
      if(doRunNow) {
        unsafeDoRun(execution, promise)
      }
      else {
        execution.streams.debug(s"Job ${execution.job.id}, is paused.")
        execution.onCancelled { () =>
          atomic { implicit tx =>
            val pausedExecutions = pausedState.get(job.id).getOrElse(Map.empty).filterKeys(_ == execution)
            pausedState += (job.id -> pausedExecutions)
          }
          promise.tryFailure(ExecutionCancelled)
        }
      }
      SubmittedExecution(execution, promise.future)
    })
  }
}
