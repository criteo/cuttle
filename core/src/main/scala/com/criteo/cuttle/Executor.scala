package com.criteo.cuttle

import java.io.{PrintWriter, StringWriter}
import java.util.{Timer, TimerTask}
import java.util.concurrent.atomic.AtomicBoolean
import java.time.{Duration, Instant, ZoneId}

import platforms.ExecutionPool

import scala.util.{Failure, Success, Try}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.stm._
import scala.concurrent.stm.Txn.ExternalDecider
import scala.concurrent.duration._
import scala.reflect.{classTag, ClassTag}
import lol.http.PartialService
import doobie.imports._
import cats.implicits._
import io.circe._

import authentication._
import logging._

trait RetryStrategy {
  def apply[S <: Scheduling](job: Job[S], context: S#Context, previouslyFailing: List[String]): Duration
  def retryWindow: Duration
}

object RetryStrategy {
  implicit def ExponentialBackoffRetryStrategy = new RetryStrategy {
    def apply[S <: Scheduling](job: Job[S], ctx: S#Context, previouslyFailing: List[String]) =
      retryWindow.multipliedBy(math.pow(2, previouslyFailing.size - 1).toLong)
    def retryWindow =
      Duration.ofMinutes(5)
  }
}

private[cuttle] sealed trait ExecutionStatus
private[cuttle] object ExecutionStatus {
  case object ExecutionSuccessful extends ExecutionStatus
  case object ExecutionFailed extends ExecutionStatus
  case object ExecutionRunning extends ExecutionStatus
  case object ExecutionPaused extends ExecutionStatus
  case object ExecutionThrottled extends ExecutionStatus
  case object ExecutionWaiting extends ExecutionStatus
  case object ExecutionTodo extends ExecutionStatus
}

private[cuttle] case class FailingJob(failedExecutions: List[ExecutionLog], nextRetry: Option[Instant]) {
  def isLastFailureAfter(date: Instant): Boolean =
    failedExecutions.headOption.flatMap(_.endTime).exists(_.isAfter(date))
}

private[cuttle] case class ExecutionLog(
  id: String,
  job: String,
  startTime: Option[Instant],
  endTime: Option[Instant],
  context: Json,
  status: ExecutionStatus,
  failing: Option[FailingJob] = None,
  waitingSeconds: Int
)

private[cuttle] class ExecutionStat(
   val startTime : Instant,
   val endTime : Instant,
   val durationSeconds : Int,
   val waitingSeconds : Int,
   val status : ExecutionStatus
)

object ExecutionCancelledException extends RuntimeException("Execution cancelled")
class CancellationListener private[cuttle](execution: Execution[_], private[cuttle] val thunk: () => Unit) {
  def unsubscribe() = execution.removeCancelListener(this)
  def unsubscribeOn[A](f: Future[A]) = f.andThen { case _ => unsubscribe() }
}

case class Execution[S <: Scheduling](
  id: String,
  job: Job[S],
  context: S#Context,
  streams: ExecutionStreams,
  platforms: Seq[ExecutionPlatform],
  executionContext: ExecutionContext
) {
  private var waitingSeconds = 0
  private[cuttle] var startTime: Option[Instant] = None
  private val cancelListeners = TSet.empty[CancellationListener]
  private val cancelled = Ref(false)

  def isCancelled = cancelled.single()
  def onCancel(thunk: () => Unit): CancellationListener = {
    val listener = new CancellationListener(this, thunk)
    val alreadyCancelled = atomic { implicit txn =>
      if(!cancelled()) {
        cancelListeners += listener
        false
      }
      else {
        true
      }
    }
    if(alreadyCancelled) {
      thunk()
    }
    listener
  }
  private[cuttle] def removeCancelListener(listener: CancellationListener): Unit = atomic { implicit txn =>
    cancelListeners -= listener
  }

  def cancel()(implicit user: User): Boolean = {
    val (hasBeenCancelled, listeners) = atomic { implicit txn =>
      if(cancelled()) {
        (false, Nil)
      } else {
        cancelled() = true
        val listeners = cancelListeners.snapshot
        cancelListeners.clear()
        (true, listeners)
      }
    }
    if(hasBeenCancelled) {
      streams.debug(s"Execution has been cancelled by user ${user.userId}.")
      listeners.foreach(listener => Try(listener.thunk()))
    }
    hasBeenCancelled
  }

  private[cuttle] def toExecutionLog(status: ExecutionStatus) =
    ExecutionLog(
      id,
      job.id,
      startTime,
      None,
      context.toJson,
      status,
      waitingSeconds = waitingSeconds
    )

  private[cuttle] def updateWaitingTime(seconds: Int): Unit =
    waitingSeconds += seconds

  private[cuttle] val isWaiting = new AtomicBoolean(false)

  def synchronize[A,B](lock: A)(thunk: => Future[B]): Future[B] = {
    if (isWaiting.get) {
      sys.error(s"Already waiting")
    } else {
      streams.debug(s"Execution waiting for lock ${lock}...")
      isWaiting.set(true)
      val pool = atomic { implicit tx =>
        if(!Execution.locks.contains(lock)) {
          Execution.locks.update(lock, new ExecutionPool(1))
        }
        Execution.locks(lock)
      }
      pool.run(this, s"Locked on ${lock}") { () =>
        streams.debug(s"Resuming")
        isWaiting.set(false)
        thunk
      }
    }
  }

  def park(duration: FiniteDuration): Future[Unit] =
    if (isWaiting.get) {
      sys.error(s"Already waiting")
    } else {
      streams.debug(s"Execution parked for $duration...")
      isWaiting.set(true)
      val p = Promise[Unit]
      p.tryCompleteWith(utils.Timeout(duration))
      this.onCancel(() => {
        p.tryComplete(Failure(ExecutionCancelledException))
      }).unsubscribeOn(p.future)
      p.future.andThen {
        case _ =>
          streams.debug(s"Resuming")
          isWaiting.set(false)
      }
    }
}

object Execution {
  private val locks = TMap.empty[Any,ExecutionPool]
  private[cuttle] implicit val ordering: Ordering[Execution[_]] =
    Ordering.by(e => (e.context: SchedulingContext, e.job.id, e.id))
  private[cuttle] implicit def ordering0[S <: Scheduling]: Ordering[Execution[S]] =
    ordering.asInstanceOf[Ordering[Execution[S]]]
}

trait ExecutionPlatform {
  def publicRoutes: PartialService = PartialFunction.empty
  def privateRoutes: AuthenticatedService = PartialFunction.empty
  def waiting: Set[Execution[_]]
}

private[cuttle] object ExecutionPlatform {
  implicit def fromExecution(implicit e: Execution[_]): Seq[ExecutionPlatform] = e.platforms
  def lookup[E: ClassTag](implicit platforms: Seq[ExecutionPlatform]): Option[E] =
    platforms.find(classTag[E].runtimeClass.isInstance).map(_.asInstanceOf[E])
}

class Executor[S <: Scheduling] private[cuttle] (
  val platforms: Seq[ExecutionPlatform],
  xa: XA,
  logger: Logger)(implicit retryStrategy: RetryStrategy) {

  val queries = new Queries {
    val appLogger = logger
  }

  private implicit val contextOrdering: Ordering[S#Context] = Ordering.by(c => c: SchedulingContext)

  import ExecutionStatus._

  private val pausedState: TMap[String, Map[Execution[S], Promise[Completed]]] = {
    val byId = TMap.empty[String, Map[Execution[S], Promise[Completed]]]
    val pausedIds = queries.getPausedJobIds.transact(xa).unsafePerformIO
    byId.single ++= pausedIds.map((_, Map.empty[Execution[S], Promise[Completed]]))
    byId
  }
  private val runningState = TMap.empty[Execution[S], Future[Completed]]
  private val throttledState = TMap.empty[Execution[S], (Promise[Completed], FailingJob)]
  private val recentFailures = TMap.empty[(Job[S], S#Context), (Option[Execution[S]], FailingJob)]
  // signals whether the instance is shutting down
  private val isShuttingDown: Ref[Boolean] = Ref(false)
  private val timer = new Timer("com.criteo.cuttle.Executor.timer")

  // executions that failed recently and are now running
  private def retryingExecutions(filteredJobs: Set[String]): Seq[(Execution[S], FailingJob, ExecutionStatus)] = {
    val runningIds = runningState.single
      .filter({ case (e, _) => filteredJobs.contains(e.job.id)})
      .map({ case (e, _) => e.job.id -> e}).toMap

    recentFailures.single
      .flatMap({ case ((job,_),(_, failingJob)) => runningIds.get(job.id).map((_, failingJob, ExecutionRunning)) })
      .toSeq
  }

  private def retryingExecutionsSize(filteredJobs: Set[String]): Int = {
    atomic {implicit txn =>
      val runningIds = runningState
        .filter({ case (e, _) => filteredJobs.contains(e.job.id)})
        .map({ case (e, _) => e.job.id }).toSet

      recentFailures.count({ case ((job,_), _) => runningIds.contains(job.id)})
    }
  }

  startMonitoringExecutions()

  private def flagWaitingExecutions(executions: Seq[Execution[S]]): Seq[(Execution[S], ExecutionStatus)] = {
    val waitings = platforms.flatMap(_.waiting).toSet
    executions.map { execution =>
      val status = if (execution.isWaiting.get || waitings.contains(execution)) ExecutionWaiting else ExecutionRunning
      (execution -> status)
    }
  }

  private[cuttle] def allRunning: Seq[ExecutionLog] =
    flagWaitingExecutions(runningState.single.keys.toSeq).map {
      case (execution, status) =>
        execution.toExecutionLog(status)
    }

  private[cuttle] def jobStatsForLastThirtyDays(jobId : String) : Seq[ExecutionStat] =
    queries.jobStatsForLastThirtyDays(jobId).transact(xa).unsafePerformIO

  private[cuttle] def runningExecutions: Seq[(Execution[S], ExecutionStatus)] =
    flagWaitingExecutions(runningState.single.keys.toSeq)

  private[cuttle] def runningExecutionsSizeTotal(filteredJobs: Set[String]): Int =
    runningState.single.keys
      .filter(e => filteredJobs.contains(e.job.id))
      .size

  private[cuttle] def runningExecutionsSizes(filteredJobs: Set[String]): (Int, Int) = {
    val statuses =
      flagWaitingExecutions(runningState.single.keys.toSeq.filter(e => filteredJobs.contains(e.job.id))).map(_._2)
    (statuses.count(_ == ExecutionRunning), statuses.count(_ == ExecutionWaiting))
  }
  private[cuttle] def runningExecutions(filteredJobs: Set[String],
                                        sort: String,
                                        asc: Boolean,
                                        offset: Int,
                                        limit: Int): Seq[ExecutionLog] =
    Seq(runningState.single.snapshot.keys.toSeq.filter(e => filteredJobs.contains(e.job.id)))
      .map(flagWaitingExecutions)
      .map { executions =>
        sort match {
          case "job" => executions.sortBy(x => (x._1.job.id, x._1))
          case "startTime" => executions.sortBy(x => (x._1.startTime.toString, x._1.id))
          case "status" => executions.sortBy(x => (x._2.toString, x._1))
          case _ => executions.sortBy(_._1)
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

  private[cuttle] def pausedExecutionsSize(filteredJobs: Set[String]): Int =
    pausedState.single.values.flatten.filter(x => filteredJobs.contains(x._1.job.id)).size
  private[cuttle] def pausedExecutions(filteredJobs: Set[String],
                                       sort: String,
                                       asc: Boolean,
                                       offset: Int,
                                       limit: Int): Seq[ExecutionLog] =
    Seq(pausedState.single.values.flatMap(_.keys).toSeq.filter(e => filteredJobs.contains(e.job.id)))
      .map { executions =>
        sort match {
          case "job" => executions.sortBy(e => (e.job.id, e))
          case "startTime" => executions.sortBy(e => (e.startTime.toString, e))
          case _ => executions.sorted
        }
      }
      .map { executions =>
        if (asc) executions else executions.reverse
      }
      .map(_.drop(offset).take(limit))
      .flatMap(_.map(_.toExecutionLog(ExecutionPaused)))

  private[cuttle] def allFailing: Set[(Job[S], S#Context)] =
    throttledState.single.keys.map(e => (e.job, e.context)).toSet
  // Count as failing all jobs that have failed and are not running (throttledState)
  // and all jobs that have recently failed and are now running.
  private[cuttle] def failingExecutionsSize(filteredJobs: Set[String]): Int =
    throttledState.single.keys.filter(e => filteredJobs.contains(e.job.id)).size + 
      retryingExecutionsSize(filteredJobs)
  private[cuttle] def allFailingExecutions: Seq[Execution[S]] =
    throttledState.single.toSeq.map(_._1)
  private[cuttle] def failingExecutions(filteredJobs: Set[String],
                                        sort: String,
                                        asc: Boolean,
                                        offset: Int,
                                        limit: Int): Seq[ExecutionLog] =
    Seq(throttledState.single.toSeq
          .filter(x => filteredJobs.contains(x._1.job.id))
          .map(x => (x._1, x._2._2, ExecutionThrottled)) ++ retryingExecutions(filteredJobs))
      .map {
        sort match {
          case "job"       =>
            _.sortBy({ case (execution, _, _) => (execution.job.id, execution) })
          case "startTime" =>
            _.sortBy({ case (execution, _, _) => (execution.startTime.toString, execution) })
          case "failed"    =>
            _.sortBy({ case (execution, failingJob, _) => (failingJob.failedExecutions.size, execution) })
          case "retry"     =>
            _.sortBy({ case (execution, failingJob, _)  => (failingJob.nextRetry.map(_.toString), execution) })
          case _ =>
            _.sortBy({ case (execution, _, _) => execution })
        }
      }
      .map { executions =>
        if (asc) executions else executions.reverse
      }
      .map(_.drop(offset).take(limit))
      .flatMap(_.map {
        case (execution, failingJob, executionStatus) =>
          execution.toExecutionLog(executionStatus).copy(failing = Some(failingJob))
      })

  private[cuttle] def archivedExecutionsSize(jobs: Set[String]): Int =
    queries.getExecutionLogSize(jobs).transact(xa).unsafePerformIO
  private[cuttle] def archivedExecutions(queryContexts: Fragment,
                                         jobs: Set[String],
                                         sort: String,
                                         asc: Boolean,
                                         offset: Int,
                                         limit: Int): Seq[ExecutionLog] =
    queries.getExecutionLog(queryContexts, jobs, sort, asc, offset, limit).transact(xa).unsafePerformIO

  private[cuttle] def pausedJobs: Seq[String] =
    pausedState.single.keys.toSeq

  private[cuttle] def cancelExecution(executionId: String)(implicit user: User): Unit = {
    val toCancel = atomic { implicit tx =>
      (runningState.keys ++ pausedState.values.flatMap(_.keys) ++ throttledState.keys).find(_.id == executionId)
    }
    toCancel.foreach(_.cancel())
  }

  private[cuttle] def getExecution(queryContexts: Fragment, executionId: String): Option[ExecutionLog] =
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
          execution.toExecutionLog(flagWaitingExecutions(execution :: Nil).head._2)
        })
    }.orElse(queries.getExecutionById(queryContexts, executionId).transact(xa).unsafePerformIO)

  private[cuttle] def openStreams(executionId: String): fs2.Stream[fs2.Task, Byte] =
    ExecutionStreams.getStreams(executionId, queries, xa)

  private[cuttle] def unpauseJobs(jobs: Set[Job[S]])(implicit user: User): Unit = {
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
          execution.startTime = Some(Instant.now())
          runningState += (execution -> promise.future)
          (execution -> promise)
      }
    }
    executionsToResume.toList.sortBy(_._1.context).foreach {
      case (execution, promise) =>
        execution.streams.debug(s"Job has been unpaused by user ${user.userId}.")
        unsafeDoRun(execution, promise)
    }
  }

  private[cuttle] def pauseJobs(jobs: Set[Job[S]])(implicit user: User): Unit = {
    val executionsToCancel = atomic { implicit tx =>
      Txn.setExternalDecider(new ExternalDecider {
        def shouldCommit(implicit txn: InTxnEnd): Boolean = {
          jobs.map(queries.pauseJob).reduceLeft(_ *> _).transact(xa).unsafePerformIO
          true
        }
      })
      jobs.flatMap { job =>
        if (!pausedState.contains(job.id)) pausedState += (job.id -> Map.empty)
        runningState.filterKeys(_.job == job).keys ++ throttledState.filterKeys(_.job == job).keys
      }
    }
    executionsToCancel.toList.sortBy(_.context).reverse.foreach { execution =>
      execution.streams.debug(s"Job has been paused by user ${user.userId}")
      execution.cancel()
    }
  }

  /**
    * Cancels all executions.
    * A hard shutdown will be performed anyway if after the
    * timeout cancelled executions still have not terminated.
    *
    * @param timeout the timeout after which a hard shutdown
    *        should be performed
    * @param user the user performing the action
    */
  private[cuttle] def gracefulShutdown(timeout: scala.concurrent.duration.Duration)(implicit user: User): Unit = {

    import utils._

    // cancel all executions
    val toCancel = atomic { implicit txn =>
      isShuttingDown() = true
      runningState.keys ++ pausedState.values.flatMap(_.keys) ++ throttledState.keys
    }
    toCancel.foreach(_.cancel())

    val runningFutures = atomic { implicit tx =>
      runningState.map({ case (k, v) => v })
    }

    Future
      .firstCompletedOf(List(Timeout(timeout), Future.sequence(runningFutures)))
      .andThen({
        case _ => hardShutdown()
      })
  }

  private[cuttle] def hardShutdown() = System.exit(0)

  private def unsafeDoRun(execution: Execution[S], promise: Promise[Completed]): Unit =
    promise.completeWith(
      execution.job
        .run(execution)
        .andThen {
          case Success(_) =>
            execution.streams.debug(s"Execution successful")
            atomic { implicit txn =>
              runningState -= execution
              recentFailures -= (execution.job -> execution.context)
            }
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
                runningState -= execution
                recentFailures += (failureKey -> (None -> failingJob.copy(failedExecutions = execution
                  .toExecutionLog(ExecutionFailed)
                  .copy(endTime = Some(Instant.now)) :: failingJob.failedExecutions)))
            }
        }
        .andThen {
          case result =>
            try {
              ExecutionStreams.archive(execution.id, queries, xa)
            } catch {
              case e: Throwable =>
                e.printStackTrace()
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

  def runAll(all: Seq[(Job[S], S#Context)]): Seq[(Execution[S], Future[Completed])] =
    run0(all.sortBy(_._2))

  def run(job: Job[S], context: S#Context): (Execution[S], Future[Completed]) =
    run0(Seq(job -> context)).head

  private def run0(all: Seq[(Job[S], S#Context)]): Seq[(Execution[S], Future[Completed])] = {
    sealed trait NewExecution
    case object ToRunNow extends NewExecution
    case object Paused extends NewExecution
    case class Throttled(launchDate: Instant) extends NewExecution

    val existingOrNew: Seq[Either[(Execution[S], Future[Completed]), (Job[S], Execution[S], Promise[Completed], NewExecution)]] =
      atomic { implicit tx =>
        if (isShuttingDown()) {
          Seq.empty
        } else
          all.map {
            case (job, context) =>
              val maybeAlreadyRunning: Option[(Execution[S], Future[Completed])] =
                runningState.find { case (e, _) => e.job == job && e.context == context }

              lazy val maybePaused: Option[(Execution[S], Future[Completed])] =
                pausedState
                  .getOrElse(job.id, Map.empty)
                  .find { case (e, _) => e.context == context }
                  .map {
                    case (e, p) => (e, p.future)
                  }

              lazy val maybeThrottled: Option[(Execution[S], Future[Completed])] =
                throttledState.find { case (e, _) => e.job == job && e.context == context }.map {
                  case (e, (p, _)) => (e, p.future)
                }

              maybeAlreadyRunning
                .orElse(maybePaused)
                .orElse(maybeThrottled)
                .toLeft {
                  val nextExecutionId = utils.randomUUID
                  val execution = Execution(
                    id = nextExecutionId,
                    job = job,
                    context = context,
                    streams = new ExecutionStreams {
                      def writeln(str: CharSequence) = ExecutionStreams.writeln(nextExecutionId, str)
                    },
                    platforms = platforms,
                    executionContext = global
                  )
                  val promise = Promise[Completed]

                  if (pausedState.contains(job.id)) {
                    val pausedExecutions = pausedState(job.id) + (execution -> promise)
                    pausedState += (job.id -> pausedExecutions)
                    (job, execution, promise, Paused)
                  } else if (recentFailures.contains(job -> context)) {
                    val (_, failingJob) = recentFailures(job -> context)
                    recentFailures += ((job -> context) -> (Some(execution) -> failingJob))
                    val throttleFor =
                      retryStrategy(job, context, recentFailures(job -> context)._2.failedExecutions.map(_.id))
                    val launchDate = failingJob.failedExecutions.head.endTime.get.plus(throttleFor)
                    throttledState += (execution -> ((promise, failingJob.copy(nextRetry = Some(launchDate)))))
                    (job, execution, promise, Throttled(launchDate))
                  } else {
                    execution.startTime = Some(Instant.now())
                    runningState += (execution -> promise.future)
                    (job, execution, promise, ToRunNow)
                  }
                }
        }
      }

    existingOrNew.map(
      _.fold(
        identity, {
          case (job, execution, promise, whatToDo) =>
            Future {
              execution.streams.debug(s"Execution: ${execution.id}")
              execution.streams.debug(s"Context: ${execution.context.toJson}")
              execution.streams.debug()
              whatToDo match {
                case ToRunNow =>
                  unsafeDoRun(execution, promise)
                case Paused =>
                  execution.streams.debug(s"Delayed because job ${execution.job.id} is paused")
                  execution.onCancel { () =>
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
                    if (cancelNow) promise.tryFailure(ExecutionCancelledException)
                  }
                case Throttled(launchDate) =>
                  execution.streams.debug(s"Delayed until ${launchDate} because previous execution failed")
                  val timerTask = new TimerTask {
                    def run = {
                      val runNow = atomic { implicit tx =>
                        if (throttledState.contains(execution)) {
                          throttledState -= execution
                          execution.startTime = Some(Instant.now())
                          runningState += execution -> promise.future
                          true
                        } else {
                          false
                        }
                      }
                      if (runNow) unsafeDoRun(execution, promise)
                    }
                  }
                  timer.schedule(timerTask, java.util.Date.from(launchDate.atZone(ZoneId.systemDefault()).toInstant))
                  execution.onCancel {
                    () =>
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
                      if (cancelNow) promise.tryFailure(ExecutionCancelledException)
                  }

              }
            }
            (execution, promise.future)
        }
      ))
  }

  private def startMonitoringExecutions() = {
    val SC = fs2.Scheduler.fromFixedDaemonPool(1, "com.criteo.cuttle.platforms.ExecutionMonitor.SC")

    val intervalSeconds = 1

    SC.scheduleAtFixedRate(intervalSeconds.second) {
      runningExecutions
        .filter({ case (_, s) => s == ExecutionStatus.ExecutionWaiting })
        .foreach({ case (e, _) => e.updateWaitingTime(intervalSeconds) })
    }
  }

  private[cuttle] def healthCheck(): Try[Boolean] =
    Try(queries.healthCheck.transact(xa).unsafePerformIO)
}
