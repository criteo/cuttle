package com.criteo.cuttle

import java.io.{PrintWriter, StringWriter}
import java.util.{Timer, TimerTask}
import java.time.{Duration, Instant, ZoneId}

import scala.util.{Failure, Success}
import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.stm._
import scala.concurrent.stm.Txn.ExternalDecider
import scala.reflect.{classTag, ClassTag}

import lol.http.{PartialService}

import doobie.imports._
import cats.implicits._

import io.circe._

trait RetryStrategy[S <: Scheduling] extends ((Job[S], S#Context, FailingJob) => Duration) {
  def retryWindow: Duration
}

object RetryStrategy {
  implicit def ExponentialBackoffRetryStrategy[A <: Scheduling] = new RetryStrategy[A] {
    def apply(job: Job[A], ctx: A#Context, failingJob: FailingJob) =
      retryWindow.multipliedBy(math.pow(2, failingJob.failedExecutions.size - 1).toLong)
    def retryWindow =
      Duration.ofMinutes(5)
  }
}

sealed trait ExecutionStatus
case object ExecutionSuccessful extends ExecutionStatus
case object ExecutionFailed extends ExecutionStatus
case object ExecutionRunning extends ExecutionStatus
case object ExecutionPaused extends ExecutionStatus
case object ExecutionThrottled extends ExecutionStatus
case object ExecutionWaiting extends ExecutionStatus
case object ExecutionTodo extends ExecutionStatus

case class FailingJob(failedExecutions: List[ExecutionLog], nextRetry: Option[Instant]) {
  def isLastFailureAfter(date: Instant): Boolean =
    failedExecutions.headOption.flatMap(_.endTime).exists(_.isAfter(date))
}

case class ExecutionLog(
  id: String,
  job: String,
  startTime: Option[Instant],
  endTime: Option[Instant],
  context: Json,
  status: ExecutionStatus,
  failing: Option[FailingJob] = None
)

object ExecutionCancelled extends RuntimeException("Execution cancelled")

case class Execution[S <: Scheduling](
  id: String,
  job: Job[S],
  context: S#Context,
  startTime: Option[Instant],
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
  def cancel(): Boolean =
    if (cancelSignal.trySuccess(())) {
      streams.debug(s"Execution has been cancelled.")
      true
    } else false

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
  def waiting: Set[Execution[S]]
}

object ExecutionPlatform {
  implicit def fromExecution[S <: Scheduling](implicit e: Execution[S]): Seq[ExecutionPlatform[S]] = e.platforms
  def lookup[E: ClassTag](implicit platforms: Seq[ExecutionPlatform[_]]): Option[E] =
    platforms.find(classTag[E].runtimeClass.isInstance).map(_.asInstanceOf[E])
}

case class Executor[S <: Scheduling](platforms: Seq[ExecutionPlatform[S]], queries: Queries, xa: XA)(
  implicit retryStrategy: RetryStrategy[S],
  contextOrdering: Ordering[S#Context]) {

  private val pausedState = {
    val byId = TMap.empty[String, Map[Execution[S], Promise[Unit]]]
    val pausedIds = queries.getPausedJobIds.transact(xa).unsafePerformIO
    byId.single ++= pausedIds.map((_, Map.empty[Execution[S], Promise[Unit]]))
    byId
  }
  private val runningState = TMap.empty[Execution[S], Future[Unit]]
  private val throttledState = TMap.empty[Execution[S], (Promise[Unit], FailingJob)]
  private val recentFailures = TMap.empty[(Job[S], S#Context), (Option[Execution[S]], FailingJob)]
  private val timer = new Timer("com.criteo.cuttle.Executor.timer")

  def allRunning: Seq[ExecutionLog] =
    runningState.single.keys.toSeq.map { execution =>
      val waiting = execution.platforms.flatMap(_.waiting).contains(execution)
      execution.toExecutionLog(if (waiting) ExecutionWaiting else ExecutionRunning)
    }

  def runningExecutions: Seq[(Execution[S], ExecutionStatus)] =
    runningState.single.keys.toSeq.map { execution =>
      val waiting = execution.platforms.flatMap(_.waiting).contains(execution)
      (execution, if (waiting) ExecutionWaiting else ExecutionRunning)
    }

  def runningExecutionsSizeTotal(filteredJobs: Set[String]): Int =
    runningState.single.keys
      .filter(e => filteredJobs.contains(e.job.id))
      .size
  def runningExecutionsSizes(filteredJobs: Set[String]): (Int, Int) = {
    val started = runningState.single.keys
      .filter(e => filteredJobs.contains(e.job.id))
    val sizes = List(true, false).map(b => started.filter(e => e.platforms.flatMap(_.waiting).contains(e) ^ b).size)
    (sizes(0), sizes(1))
  }
  def runningExecutions(filteredJobs: Set[String],
                        sort: String,
                        asc: Boolean,
                        offset: Int,
                        limit: Int): Seq[ExecutionLog] =
    Seq(runningState.single.snapshot.keys.toSeq.filter(e => filteredJobs.contains(e.job.id)))
      .map(_.map { execution =>
        val waiting = execution.platforms.flatMap(_.waiting).contains(execution)
        (execution, if (waiting) ExecutionWaiting else ExecutionRunning)
      })
      .map { executions =>
        sort match {
          case "job" => executions.sortBy(_._1.job.id)
          case "startTime" => executions.sortBy(_._1.startTime.toString)
          case "status" => executions.sortBy(_._2.toString)
          case _ => executions.sortBy(_._1.context)
        }
      }
      .map { executions =>
        if (asc) executions else executions.reverse
      }
      .map(_.drop(offset).take(limit))
      .flatMap(_.map {
        case (execution, status) =>
          execution.toExecutionLog(status)
      })

  def pausedExecutionsSize(filteredJobs: Set[String]): Int =
    pausedState.single.values.flatten.filter(x => filteredJobs.contains(x._1.job.id)).size
  def pausedExecutions(filteredJobs: Set[String],
                       sort: String,
                       asc: Boolean,
                       offset: Int,
                       limit: Int): Seq[ExecutionLog] =
    Seq(pausedState.single.values.flatMap(_.keys).toSeq.filter(e => filteredJobs.contains(e.job.id)))
      .map { executions =>
        sort match {
          case "job" => executions.sortBy(_.job.id)
          case "startTime" => executions.sortBy(_.startTime.toString)
          case _ => executions.sortBy(_.context)
        }
      }
      .map { executions =>
        if (asc) executions else executions.reverse
      }
      .map(_.drop(offset).take(limit))
      .flatMap(_.map(_.toExecutionLog(ExecutionPaused)))

  def allFailing: Set[(Job[S], S#Context)] =
    throttledState.single.keys.map(e => (e.job, e.context)).toSet
  def failingExecutionsSize(filteredJobs: Set[String]): Int =
    throttledState.single.keys.filter(e => filteredJobs.contains(e.job.id)).size
  def allFailingExecutions: Seq[Execution[S]] =
    throttledState.single.toSeq.map(_._1)
  def failingExecutions(filteredJobs: Set[String],
                        sort: String,
                        asc: Boolean,
                        offset: Int,
                        limit: Int): Seq[ExecutionLog] =
    Seq(throttledState.single.toSeq.filter(x => filteredJobs.contains(x._1.job.id)))
      .map { executions =>
        sort match {
          case "job" => executions.sortBy(_._1.job.id)
          case "startTime" => executions.sortBy(_._1.startTime.toString)
          case "failed" => executions.sortBy(_._2._2.failedExecutions.size)
          case "retry" => executions.sortBy(_._2._2.nextRetry.map(_.toString))
          case _ => executions.sortBy(_._1.context)
        }
      }
      .map { executions =>
        if (asc) executions else executions.reverse
      }
      .map(_.drop(offset).take(limit))
      .flatMap(_.map {
        case (execution, (_, failingJob)) =>
          execution.toExecutionLog(ExecutionThrottled).copy(failing = Some(failingJob))
      })

  def archivedExecutionsSize(jobs: Set[String]): Int =
    queries.getExecutionLogSize(jobs).transact(xa).unsafePerformIO
  def archivedExecutions(queryContexts: Fragment,
                         jobs: Set[String],
                         sort: String,
                         asc: Boolean,
                         offset: Int,
                         limit: Int): Seq[ExecutionLog] =
    queries.getExecutionLog(queryContexts, jobs, sort, asc, offset, limit).transact(xa).unsafePerformIO

  def pausedJobs: Seq[String] =
    pausedState.single.keys.toSeq

  def cancelExecution(executionId: String): Unit = {
    val toCancel = atomic { implicit tx =>
      (runningState.keys ++ pausedState.values.flatMap(_.keys) ++ throttledState.keys).find(_.id == executionId)
    }
    toCancel.foreach(_.cancel())
  }

  def getExecution(queryContexts: Fragment, executionId: String): Option[ExecutionLog] =
    atomic { implicit tx =>
      val predicate = (e: Execution[S]) => e.id == executionId
      pausedState.values
        .map(_.keys)
        .flatten
        .find(predicate)
        .map(_.toExecutionLog(ExecutionPaused))
        .orElse(throttledState.keys
          .find(predicate)
          .map(e => e.toExecutionLog(ExecutionThrottled).copy(failing = throttledState.get(e).map(_._2))))
        .orElse(runningState.keys.find(predicate).map { execution =>
          val waiting = execution.platforms.flatMap(_.waiting).contains(execution)
          execution.toExecutionLog(if (waiting) ExecutionWaiting else ExecutionRunning)
        })
    }.orElse(queries.getExecutionById(queryContexts, executionId).transact(xa).unsafePerformIO)

  def openStreams(executionId: String): fs2.Stream[fs2.Task, Byte] =
    ExecutionStreams.getStreams(executionId, queries, xa)

  def unpauseJobs(jobs: Set[Job[S]]): Unit = {
    val executionsToResume = atomic { implicit tx =>
      Txn.setExternalDecider(new ExternalDecider {
        def shouldCommit(implicit tx: InTxnEnd): Boolean = {
          jobs.map(queries.unpauseJob).reduceLeft(_ *> _).transact(xa).unsafePerformIO
          true
        }
      })
      val executions = jobs.flatMap(job => pausedState.get(job.id).map(_.toSeq).getOrElse(Nil))
      jobs.foreach(job => pausedState -= job.id)
      executions.map {
        case (execution, promise) =>
          val startedExecution = execution.copy(startTime = Some(Instant.now()))
          runningState += (startedExecution -> promise.future)
          (startedExecution -> promise)
      }
    }
    executionsToResume.toList.sortBy(_._1.context).foreach {
      case (execution, promise) =>
        execution.streams.debug(s"Job has been unpaused.")
        unsafeDoRun(execution, promise)
    }
  }

  def pauseJobs(jobs: Set[Job[S]]): Unit = {
    val executionsToCancel = atomic { implicit tx =>
      Txn.setExternalDecider(new ExternalDecider {
        def shouldCommit(implicit txn: InTxnEnd): Boolean = {
          jobs.map(queries.pauseJob).reduceLeft(_ *> _).transact(xa).unsafePerformIO
          true
        }
      })
      jobs.flatMap { job =>
        pausedState += (job.id -> Map.empty)
        runningState.filterKeys(_.job == job).keys ++ throttledState.filterKeys(_.job == job).keys
      }
    }
    executionsToCancel.toList.sortBy(_.context).reverse.foreach { execution =>
      execution.streams.debug(s"Job has been paused")
      execution.cancel()
    }
  }

  private def unsafeDoRun(execution: Execution[S], promise: Promise[Unit]): Unit =
    promise.completeWith(
      execution.job
        .effect(execution)
        .andThen {
          case Success(()) =>
            execution.streams.debug(s"Execution successful")
            recentFailures.single -= (execution.job -> execution.context)
          case Failure(e) =>
            val stacktrace = new StringWriter()
            e.printStackTrace(new PrintWriter(stacktrace))
            execution.streams.error(s"Execution failed:")
            execution.streams.error(stacktrace.toString)
            atomic {
              implicit tx =>
                recentFailures.retain {
                  case (_, (retryExecution, failingJob)) =>
                    retryExecution.isDefined || failingJob.isLastFailureAfter(
                      Instant.now.minus(retryStrategy.retryWindow))
                }
                val failureKey = (execution.job, execution.context)
                val failingJob = recentFailures.get(failureKey).map(_._2).getOrElse(FailingJob(Nil, None))
                recentFailures += (failureKey -> (None -> failingJob.copy(failedExecutions = execution
                  .toExecutionLog(ExecutionFailed)
                  .copy(endTime = Some(Instant.now)) :: failingJob.failedExecutions)))
            }
        }
        .andThen {
          case result =>
            ExecutionStreams.archive(execution.id, queries, xa)
            atomic { implicit tx =>
              runningState -= execution
            }
            if (execution.startTime.isDefined) {
              queries
                .logExecution(
                  execution
                    .toExecutionLog(if (result.isSuccess) ExecutionSuccessful else ExecutionFailed)
                    .copy(endTime = Some(Instant.now)),
                  execution.context.log
                )
                .transact(xa)
                .unsafePerformIO
            }
        })

  def runAll(all: Seq[(Job[S], S#Context)]): Seq[SubmittedExecution[S]] = all.sortBy(_._2).map((run _).tupled)

  def run(job: Job[S], context: S#Context): SubmittedExecution[S] = {
    sealed trait NewExecution
    case object ToRunNow extends NewExecution
    case object Paused extends NewExecution
    case class Throttled(launchDate: Instant) extends NewExecution

    val existingOrNew: Either[SubmittedExecution[S], (Execution[S], Promise[Unit], NewExecution)] = atomic {
      implicit tx =>
        val maybeAlreadyRunning: Option[(Execution[S], Future[Unit])] =
          runningState.find { case (e, _) => e.job == job && e.context == context }

        lazy val maybePaused: Option[(Execution[S], Future[Unit])] =
          pausedState
            .getOrElse(job.id, Map.empty)
            .find { case (e, _) => e.context == context }
            .map {
              case (e, p) => (e, p.future)
            }

        lazy val maybeThrottled: Option[(Execution[S], Future[Unit])] =
          throttledState.find { case (e, _) => e.job == job && e.context == context }.map {
            case (e, (p, _)) => (e, p.future)
          }

        maybeAlreadyRunning
          .orElse(maybePaused)
          .orElse(maybeThrottled)
          .map((SubmittedExecution.apply[S] _).tupled)
          .toLeft {
            val nextExecutionId = utils.randomUUID
            val execution = Execution(
              id = nextExecutionId,
              job = job,
              context = context,
              startTime = None,
              streams = new ExecutionStreams {
                def writeln(str: CharSequence) = ExecutionStreams.writeln(nextExecutionId, str)
              },
              platforms = platforms
            )
            val promise = Promise[Unit]

            if (pausedState.contains(job.id)) {
              val pausedExecutions = pausedState(job.id) + (execution -> promise)
              pausedState += (job.id -> pausedExecutions)
              (execution, promise, Paused)
            } else if (recentFailures.contains(job -> context)) {
              val (_, failingJob) = recentFailures(job -> context)
              recentFailures += ((job -> context) -> (Some(execution) -> failingJob))
              val throttleFor = retryStrategy(job, context, recentFailures(job -> context)._2)
              val launchDate = failingJob.failedExecutions.head.endTime.get.plus(throttleFor)
              throttledState += (execution -> ((promise, failingJob.copy(nextRetry = Some(launchDate)))))
              (execution, promise, Throttled(launchDate))
            } else {
              val startedExecution = execution.copy(startTime = Some(Instant.now()))
              runningState += (startedExecution -> promise.future)
              (startedExecution, promise, ToRunNow)
            }
          }
    }

    existingOrNew.fold(
      identity, {
        case (execution, promise, whatToDo) =>
          execution.streams.debug(s"Execution: ${execution.id}")
          execution.streams.debug(s"Context: ${execution.context.toJson}")
          whatToDo match {
            case ToRunNow =>
              unsafeDoRun(execution, promise)
            case Paused =>
              execution.streams.debug(s"Delayed because job ${execution.job.id} is paused")
              execution.onCancelled { () =>
                val cancelNow = atomic { implicit tx =>
                  val isStillPaused = pausedState.get(job.id).getOrElse(Map.empty).contains(execution)
                  if (isStillPaused) {
                    val pausedExecutions = pausedState.get(job.id).getOrElse(Map.empty).filterKeys(_ == execution)
                    pausedState += (job.id -> pausedExecutions)
                    true
                  } else {
                    false
                  }
                }
                if (cancelNow) promise.tryFailure(ExecutionCancelled)
              }
            case Throttled(launchDate) =>
              execution.streams.debug(s"Delayed until ${launchDate} because previous execution failed")
              val timerTask = new TimerTask {
                def run = {
                  val runNow = atomic { implicit tx =>
                    if (throttledState.contains(execution)) {
                      throttledState -= execution
                      true
                    } else {
                      false
                    }
                  }
                  if (runNow) unsafeDoRun(execution, promise)
                }
              }
              timer.schedule(timerTask, java.util.Date.from(launchDate.atZone(ZoneId.systemDefault()).toInstant))
              execution.onCancelled { () =>
                val cancelNow = atomic { implicit tx =>
                  val failureKey = (execution.job -> execution.context)
                  recentFailures.get(failureKey).foreach {
                    case (_, failingJob) =>
                      recentFailures += (failureKey -> (None -> failingJob))
                  }
                  val isStillDelayed = throttledState.contains(execution)
                  if (isStillDelayed) {
                    throttledState -= execution
                    true
                  } else {
                    false
                  }
                }
                if (cancelNow) promise.tryFailure(ExecutionCancelled)
              }

          }
          SubmittedExecution(execution, promise.future)
      }
    )
  }
}
