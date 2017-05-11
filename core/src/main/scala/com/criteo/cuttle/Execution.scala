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

object ExecutionCancelled extends RuntimeException("Execution cancelled.")

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
  def cancelled = cancelSignal.future
  def onCancelled(thunk: () => Unit) = cancelled.andThen {
    case Success(_) => thunk()
    case Failure(_) =>
  }
  def cancel(): Boolean = cancelSignal.trySuccess(())

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

case class Executor[S <: Scheduling](platforms: Seq[ExecutionPlatform[S]], queries: Queries, xa: XA) {

  private val pausedState = {
    val byId = TMap.empty[String, Map[Execution[S], Promise[Unit]]]
    val pausedIds = queries.getPausedJobIds.transact(xa).unsafePerformIO
    byId.single ++= pausedIds.map((_, Map.empty[Execution[S], Promise[Unit]]))
    byId
  }

  private val runningState = TSet.empty[Execution[S]]

  def runningExecutions: Seq[ExecutionLog] =
    runningState.single.snapshot.toSeq.map { execution =>
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

  def cancelExecution(executionId: String): Unit =
    runningState.single.find(_.id == executionId).foreach(_.cancel())

  def unpauseJob(job: Job[S]): Unit = {
    val executions = atomic { implicit txn =>
      Txn.setExternalDecider(new ExternalDecider {
        def shouldCommit(implicit txn: InTxnEnd): Boolean = {
          queries.unpauseJob(job).transact(xa).unsafePerformIO
          true
        }
      })
      val excs = pausedState.get(job.id)
      pausedState -= job.id
      excs
    }
    executions.toList.flatMap(_.toList).foreach {
      case (execution, promise) =>
        promise.completeWith(run0(execution))
    }
  }

  def pauseJob(job: Job[S]): Unit =
    atomic { implicit txn =>
      Txn.setExternalDecider(new ExternalDecider {
        def shouldCommit(implicit txn: InTxnEnd): Boolean = {
          queries.pauseJob(job).transact(xa).unsafePerformIO
          true
        }
      })
      pausedState += (job.id -> Map.empty)
      runningState.filter(_.job == job).foreach(_.cancel())
    }

  private def run0(execution: Execution[S]): Future[Unit] = {
    atomic { implicit tx => runningState += execution }
    execution.job.effect(execution).andThen {
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
    }
  }

  def run(job: Job[S], context: S#Context): SubmittedExecution[S] = {
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

    val maybePausedExecution: Option[Future[Unit]] = atomic { implicit txn =>
      for {
        executions <- pausedState.get(job.id)
      } yield {
        val promise = Promise[Unit]()
        pausedState.update(job.id, executions + (execution -> promise))
        execution.onCancelled { () =>
          promise.tryFailure(ExecutionCancelled)
          val executions = pausedState.get(job.id).getOrElse(Map.empty).filterKeys(_ == execution)
          pausedState.update(job.id, executions)
        }
        promise.future
      }
    }

    SubmittedExecution(execution, maybePausedExecution.getOrElse(run0(execution)))
  }
}
