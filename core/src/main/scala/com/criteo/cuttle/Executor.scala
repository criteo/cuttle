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
import io.circe._
import io.circe.syntax._
import Auth._
import Metrics._
import cats.Eq
import cats.implicits._
import cats.effect.IO
import doobie.util.fragment.Fragment
import doobie.implicits._

/** The strategy to use to retry stuck executions.
  *
  * When an [[Execution]] fails, the [[Executor]] keeps a track of this failure in a list of
  * recently failed [[Execution Executions]].
  *
  * If the [[Scheduler]] asks for the same [[Execution]] (same here is defined as an execution for the same
  * [[Job]] and the same [[SchedulingContext]]) during the `retryWindow` defined by this strategy, it will be
  * considered by the [[Executor]] as a __stuck__ execution and delayed for the duration computed by the
  * configured [[RetryStrategy]].
  */
trait RetryStrategy {

  /** Compute the duration this stuck execution should be delayed by. During this time the execution will
    * appear in the __stuck__ screen and the [[SideEffect]] function won't be called.
    *
    * @param job The job for which the execution is stuck.
    * @param context The [[SchedulingContext]] for which the execution is stuck.
    * @param previouslyFailing The previous failure for the same (job,context).
    * @return The duration this execution should be delayed by.
    */
  def apply[S <: Scheduling](job: Job[S], context: S#Context, previouslyFailing: List[String]): Duration

  /** The retry window considere by this strategy. */
  def retryWindow: Duration
}

/** Built-in [[RetryStrategy]] */
object RetryStrategy {

  /** Exponential backoff strategy. First retry will be delayed for 5 minutes, second one by 10 minutes,
    * next one by 20 minutes, ..., n one by 5 minutes *  2^(n-1). */
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
  val startTime: Instant,
  val endTime: Instant,
  val durationSeconds: Int,
  val waitingSeconds: Int,
  val status: ExecutionStatus
)

private[cuttle] object ExecutionLog {
  implicit val execLogEq: Eq[ExecutionLog] = Eq.fromUniversalEquals[ExecutionLog]
}

/**
  * Used to fail an execution Future when the execution has been cancelled.
  */
object ExecutionCancelled extends RuntimeException("Execution cancelled")

/**
  * Allows to unsubscribe a cancel listener. It is sometimes useful for long running executions to dettach
  * the cancel listener to not leak any memory.
  */
class CancellationListener private[cuttle] (execution: Execution[_], private[cuttle] val thunk: () => Unit) {

  /**
    * Detach the cancellation listener. The previously attached listener won't be notified anymore in case
    * of execution cancel.
    */
  def unsubscribe() = execution.removeCancelListener(this)

  /**
    * Detach the cancellation listener as soon as the specified future is completed (either with a success
    * or a failure).
    */
  def unsubscribeOn[A](f: Future[A]) = f.andThen { case _ => unsubscribe() }
}

/** [[Execution Executions]] are created by the [[Scheduler]].
  *
  * @param id The unique id for this execution. Guaranteed to be unique.
  * @param job The [[Job]] for which this execution has been created.
  * @param context The [[SchedulingContext]] for which this execution has been created.
  * @param streams The execution streams are scoped stdout, stderr for the execution.
  * @param platforms The available [[ExecutionPlatform ExecutionPlatforms]] for this execution.
  * @param executionContext The scoped `scala.concurrent.ExecutionContext` for this execution.
  */
case class Execution[S <: Scheduling](
  id: String,
  job: Job[S],
  context: S#Context,
  streams: ExecutionStreams,
  platforms: Seq[ExecutionPlatform],
  executionContext: ExecutionContext,
  projectName: String
) {
  private var waitingSeconds = 0
  private[cuttle] var startTime: Option[Instant] = None
  private val cancelListeners = TSet.empty[CancellationListener]
  private val cancelled = Ref(false)

  /** Returns `true` if this [[Execution]] has been cancelled. */
  def isCancelled = cancelled.single()

  /** Attach a [[CancellationListener]] that will be called id this [[Execution]]
    * is cancelled.
    *
    * @param thunk The callback function to be called.
    */
  def onCancel(thunk: () => Unit): CancellationListener = {
    val listener = new CancellationListener(this, thunk)
    val alreadyCancelled = atomic { implicit txn =>
      if (!cancelled()) {
        cancelListeners += listener
        false
      } else {
        true
      }
    }
    if (alreadyCancelled) {
      thunk()
    }
    listener
  }
  private[cuttle] def removeCancelListener(listener: CancellationListener): Unit = atomic { implicit txn =>
    cancelListeners -= listener
  }

  /** Cancels this [[Execution]]. Note that cancelling an execution does not forcibly stop the [[SideEffect]] function
    * if it has already started. It will just call the [[CancellationListener]] so the user code can gracefully shutdown
    * if it handles cancellation properly. Note that the provided [[ExecutionPlatform ExecutionPlatforms]] handle cancellation properly, so
    * for [[SideEffect]] that use the provided platforms they support cancellation out of the box.
    *
    * @param user The user who asked for the cancellation (either from the UI or the private API).
    */
  def cancel()(implicit user: User): Boolean = {
    val (hasBeenCancelled, listeners) = atomic { implicit txn =>
      if (cancelled()) {
        (false, Nil)
      } else {
        cancelled() = true
        val listeners = cancelListeners.snapshot
        cancelListeners.clear()
        (true, listeners)
      }
    }
    if (hasBeenCancelled) {
      streams.debug(s"Execution has been cancelled by user ${user.userId}.")
      listeners.foreach(listener => Try(listener.thunk()))
    }
    hasBeenCancelled
  }

  private[cuttle] def toExecutionLog(status: ExecutionStatus, failing: Option[FailingJob] = None) =
    ExecutionLog(
      id,
      job.id,
      startTime,
      None,
      context.toJson,
      status,
      waitingSeconds = waitingSeconds,
      failing = failing
    )

  private[cuttle] def updateWaitingTime(seconds: Int): Unit =
    waitingSeconds += seconds

  private[cuttle] val isWaiting = new AtomicBoolean(false)

  /** Synchronize a code block over a lock. If several [[SideEffect]] functions need to race
    * for a shared thread unsafe resource, they can use this helper function to ensure that only
    * one code block with run at once. Think about it as an asynchronous `synchronized` helper.
    *
    * While waiting for the lock, the [[Execution]] will be seen as __WAITING__ in the UI and the API.
    */
  def synchronize[A, B](lock: A)(thunk: => Future[B]): Future[B] =
    if (isWaiting.get) {
      sys.error(s"Already waiting")
    } else {
      streams.debug(s"Execution waiting for lock ${lock}...")
      isWaiting.set(true)
      val pool = atomic { implicit tx =>
        if (!Execution.locks.contains(lock)) {
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

  /** Park this execution for the provided duration. After the duration
    * the returned `Future` will complete allowing the [[SideEffect]] to resume.
    *
    * During this time, the [[Execution]] will be seen as __WAITING__ in the UI and the API.
    */
  def park(duration: FiniteDuration): Future[Unit] =
    if (isWaiting.get) {
      sys.error(s"Already waiting")
    } else {
      streams.debug(s"Execution parked for $duration...")
      isWaiting.set(true)
      val p = Promise[Unit]
      p.tryCompleteWith(utils.Timeout(duration))
      this
        .onCancel(() => {
          p.tryComplete(Failure(ExecutionCancelled))
        })
        .unsubscribeOn(p.future)
      p.future.andThen {
        case _ =>
          streams.debug(s"Resuming")
          isWaiting.set(false)
      }
    }

  /**
    * Run this execution on its job.
    */
  private[cuttle] def run(): Future[Completed] =
    job.run(this)
}

private[cuttle] object Execution {
  private val locks = TMap.empty[Any, ExecutionPool]
  implicit val ordering: Ordering[Execution[_]] =
    Ordering.by(e => (e.context: SchedulingContext, e.job.id, e.id))
  implicit def ordering0[S <: Scheduling]: Ordering[Execution[S]] =
    ordering.asInstanceOf[Ordering[Execution[S]]]
}

/** An [[ExecutionPlatform]] provides controlled access to shared resources. */
trait ExecutionPlatform {

  /** Expose a public `lolhttp` service for the platform internal statistics (for the UI and API). */
  def publicRoutes: PartialService = PartialFunction.empty

  /** Expose a private `lolhttp` service for the platform operations (for the UI and API). */
  def privateRoutes: AuthenticatedService = PartialFunction.empty

  /** @return the list of [[Execution]] waiting for resources on this platform.
    * These executions will be seen as __WAITING__ in the UI and the API. */
  def waiting: Set[Execution[_]]
}

private[cuttle] object ExecutionPlatform {
  implicit def fromExecution(implicit e: Execution[_]): Seq[ExecutionPlatform] = e.platforms
  def lookup[E: ClassTag](implicit platforms: Seq[ExecutionPlatform]): Option[E] =
    platforms.find(classTag[E].runtimeClass.isInstance).map(_.asInstanceOf[E])
}

/** An [[Executor]] is responsible to actually execute the [[SideEffect]] functions for the
  * given [[Execution Executions]]. */
class Executor[S <: Scheduling] private[cuttle] (val platforms: Seq[ExecutionPlatform],
                                                 xa: XA,
                                                 logger: Logger,
                                                 projectName: String)(implicit retryStrategy: RetryStrategy)
    extends MetricProvider[S] {

  import ExecutionStatus._

  private implicit val contextOrdering: Ordering[S#Context] = Ordering.by(c => c: SchedulingContext)

  private val queries = new Queries {
    val appLogger: Logger = logger
  }

  private val pausedState: TMap[String, Map[Execution[S], Promise[Completed]]] = {
    val byId = TMap.empty[String, Map[Execution[S], Promise[Completed]]]
    val pausedIds = queries.getPausedJobIds.transact(xa).unsafeRunSync
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
  private def retryingExecutions(filteredJobs: Set[String]): Seq[(Execution[S], FailingJob, ExecutionStatus)] =
    atomic { implicit txn =>
      val runningIds = runningState.collect {
        case (e: Execution[S], _) if filteredJobs.contains(e.job.id) => (e.job.id, e.context) -> e
      }

      recentFailures
        .flatMap({
          case ((job, context), (_, failingJob)) =>
            runningIds.get((job.id, context)).map((_, failingJob, ExecutionRunning))
        })
        .toSeq
    }

  private def retryingExecutionsSize(filteredJobs: Set[String]): Int =
    atomic { implicit txn =>
      val runningIds = runningState.toSeq.collect {
        case (e: Execution[S], _) if filteredJobs.contains(e.job.id) => (e.job.id, e.context)
      }

      recentFailures.count({ case ((job, context), _) => runningIds.contains((job.id, context)) })
    }

  private def startMonitoringExecutions() = {
    val SC = utils.createScheduler("com.criteo.cuttle.platforms.ExecutionMonitor.SC")

    val intervalSeconds = 1
    SC.awakeEvery[IO](intervalSeconds.second)
      .map(_ => {
        runningExecutions
          .filter({ case (_, s) => s == ExecutionStatus.ExecutionWaiting })
          .foreach({ case (e, _) => e.updateWaitingTime(intervalSeconds) })
      })
      .run
      .unsafeRunAsync(_ => ())
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

  private[cuttle] def jobStatsForLastThirtyDays(jobId: String): IO[Seq[ExecutionStat]] =
    queries.jobStatsForLastThirtyDays(jobId).transact(xa)

  private[cuttle] def runningExecutions: Seq[(Execution[S], ExecutionStatus)] =
    flagWaitingExecutions(runningState.single.keys.toSeq)

  private[cuttle] def runningExecutionsSizeTotal(filteredJobs: Set[String]): Int =
    runningState.single.keys.count(e => filteredJobs.contains(e.job.id))

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
          case "job"       => executions.sortBy(x => (x._1.job.id, x._1))
          case "startTime" => executions.sortBy(x => (x._1.startTime.toString, x._1.id))
          case "status"    => executions.sortBy(x => (x._2.toString, x._1))
          case _           => executions.sortBy(_._1)
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
          case "job"       => executions.sortBy(e => (e.job.id, e))
          case "startTime" => executions.sortBy(e => (e.startTime.toString, e))
          case _           => executions.sorted
        }
      }
      .map { executions =>
        if (asc) executions else executions.reverse
      }
      .map(_.drop(offset).take(limit))
      .flatMap(_.map(_.toExecutionLog(ExecutionPaused)))

  private[cuttle] def allFailingExecutions: Seq[Execution[S]] =
    throttledState.single.keys.toSeq

  private[cuttle] def allFailingJobsWithContext: Set[(Job[S], S#Context)] =
    allFailingExecutions.map(e => (e.job, e.context)).toSet

  // Count as failing all jobs that have failed and are not running (throttledState)
  // and all jobs that have recently failed and are now running.
  private[cuttle] def failingExecutionsSize(filteredJobs: Set[String]): Int =
    throttledState.single.keys.filter(e => filteredJobs.contains(e.job.id)).size +
      retryingExecutionsSize(filteredJobs)

  private[cuttle] def failingExecutions(filteredJobs: Set[String],
                                        sort: String,
                                        asc: Boolean,
                                        offset: Int,
                                        limit: Int): Seq[ExecutionLog] =
    Seq(
      throttledState.single.toSeq
        .filter(x => filteredJobs.contains(x._1.job.id))
        .map(x => (x._1, x._2._2, ExecutionThrottled)) ++ retryingExecutions(filteredJobs))
      .map {
        sort match {
          case "job" =>
            _.sortBy({ case (execution, _, _) => (execution.job.id, execution) })
          case "startTime" =>
            _.sortBy({ case (execution, _, _) => (execution.startTime.toString, execution) })
          case "failed" =>
            _.sortBy({ case (execution, failingJob, _) => (failingJob.failedExecutions.size, execution) })
          case "retry" =>
            _.sortBy({ case (execution, failingJob, _) => (failingJob.nextRetry.map(_.toString), execution) })
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

  private[cuttle] def archivedExecutionsSize(jobs: Set[String]): IO[Int] =
    queries.getExecutionLogSize(jobs).transact(xa)

  private[cuttle] def archivedExecutions(queryContexts: Fragment,
                                         jobs: Set[String],
                                         sort: String,
                                         asc: Boolean,
                                         offset: Int,
                                         limit: Int,
                                         xa: XA): IO[Seq[ExecutionLog]] =
    queries.getExecutionLog(queryContexts, jobs, sort, asc, offset, limit).transact(xa)

  private[cuttle] def pausedJobs: Seq[String] =
    pausedState.single.keys.toSeq

  private[cuttle] def cancelExecution(executionId: String)(implicit user: User): Unit = {
    val toCancel = atomic { implicit tx =>
      (runningState.keys ++ pausedState.values.flatMap(_.keys) ++ throttledState.keys).find(_.id == executionId)
    }
    toCancel.foreach(_.cancel())
  }

  private[cuttle] def getExecution(queryContexts: Fragment, executionId: String): IO[Option[ExecutionLog]] =
    atomic { implicit tx =>
      val predicate = (e: Execution[S]) => e.id == executionId
      pausedState.values
        .flatMap(_.keys)
        .find(predicate)
        .map(_.toExecutionLog(ExecutionPaused))
        .orElse(throttledState.keys
          .find(predicate)
          .map(e => e.toExecutionLog(ExecutionThrottled).copy(failing = throttledState.get(e).map(_._2))))
        .orElse(runningState.keys.find(predicate).map { execution =>
          execution.toExecutionLog(flagWaitingExecutions(execution :: Nil).head._2)
        })
    } match {
      case None                      => queries.getExecutionById(queryContexts, executionId).transact(xa)
      case (a: Option[ExecutionLog]) => IO.pure(a)
    }

  private[cuttle] def openStreams(executionId: String): fs2.Stream[IO, Byte] =
    ExecutionStreams.getStreams(executionId, queries, xa)

  private[cuttle] def pauseJobs(jobs: Set[Job[S]])(implicit user: User): Unit = {
    val executionsToCancel = atomic { implicit tx =>
      Txn.setExternalDecider(new ExternalDecider {
        def shouldCommit(implicit txn: InTxnEnd): Boolean = {
          jobs.map(queries.pauseJob).reduceLeft(_ *> _).transact(xa).unsafeRunSync
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

  private[cuttle] def resumeJobs(jobs: Set[Job[S]])(implicit user: User): Unit = {
    val executionsToResume = atomic { implicit tx =>
      Txn.setExternalDecider(new ExternalDecider {
        def shouldCommit(implicit tx: InTxnEnd): Boolean = {
          jobs.map(queries.unpauseJob).reduceLeft(_ *> _).transact(xa).unsafeRunSync
          true
        }
      })
      val executions = jobs.flatMap(job => pausedState.get(job.id).map(_.toSeq).getOrElse(Nil))
      jobs.foreach(job => pausedState -= job.id)
      executions.map {
        case (execution, promise) =>
          addExecution2RunningState(execution, promise)
          (execution -> promise)
      }
    }
    executionsToResume.toList.sortBy(_._1.context).foreach {
      case (execution, promise) =>
        execution.streams.debug(s"Job has been resumed by user ${user.userId}.")
        unsafeDoRun(execution, promise)
    }
  }

  private[cuttle] def relaunch(jobs: Set[String])(implicit user: User): Unit = {
    val execution2Promise = atomic { implicit txn =>
      throttledState.collect {
        case (execution, (promise, _)) if jobs.contains(execution.job.id) =>
          throttledState -= execution
          addExecution2RunningState(execution, promise)
          execution -> promise
      }.toSeq
    }

    execution2Promise.foreach {
      case (execution, promise) =>
        execution.streams.debug(s"Job has been relaunched by user ${user.userId}.")
        unsafeDoRun(execution, promise)
    }
  }

  private[cuttle] def gracefulShutdown(timeout: scala.concurrent.duration.Duration)(implicit user: User): Unit = {

    import utils._

    // cancel all executions
    val toCancel = atomic { implicit txn =>
      isShuttingDown() = true
      runningState.keys ++ pausedState.values.flatMap(_.keys) ++ throttledState.keys
    }
    toCancel.foreach(_.cancel())

    val runningFutures = atomic { implicit tx =>
      runningState.map({ case (_, v) => v })
    }

    Future
      .firstCompletedOf(List(Timeout(timeout), Future.sequence(runningFutures)))
      .andThen({
        case _ => hardShutdown()
      })
  }

  private[cuttle] def hardShutdown() = System.exit(0)

  private def addExecution2RunningState(execution: Execution[S], promise: Promise[Completed]): Unit =
    atomic { implicit txn =>
      execution.startTime = Some(Instant.now())
      runningState += (execution -> promise.future)
    }

  private def unsafeDoRun(execution: Execution[S], promise: Promise[Completed]): Unit =
    promise.completeWith(
      (Try(execution.run()) match {
        case Success(f) => f
        case Failure(e) => Future.failed(e)
      }).andThen {
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
                // retain jobs in recent failures if last failure happened in [now - retryStrategy.retryWindow, now]
                recentFailures.retain {
                  case (_, (retryExecution, failingJob)) =>
                    retryExecution.isDefined || failingJob.isLastFailureAfter(
                      Instant.now.minus(retryStrategy.retryWindow))
                }
                val failureKey = (execution.job, execution.context)
                val failingJob = recentFailures.get(failureKey).map(_._2).getOrElse(FailingJob(Nil, None))
                runningState -= execution
                val failedExecutions = execution
                  .toExecutionLog(ExecutionFailed)
                  .copy(endTime = Some(Instant.now)) :: failingJob.failedExecutions
                recentFailures += (failureKey -> (None -> failingJob.copy(failedExecutions = failedExecutions)))
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
                .unsafeRunSync
            }
        })

  private def run0(all: Seq[(Job[S], S#Context)]): Seq[(Execution[S], Future[Completed])] = {
    sealed trait NewExecution
    case object ToRunNow extends NewExecution
    case object Paused extends NewExecution
    case class Throttled(launchDate: Instant) extends NewExecution

    val existingOrNew
      : Seq[Either[(Execution[S], Future[Completed]), (Job[S], Execution[S], Promise[Completed], NewExecution)]] =
      atomic { implicit txn =>
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
                    executionContext = global,
                    projectName = projectName
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
                    addExecution2RunningState(execution, promise)
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
                    if (cancelNow) promise.tryFailure(ExecutionCancelled)
                  }
                case Throttled(launchDate) =>
                  execution.streams.debug(s"Delayed until $launchDate because previous execution failed")
                  val timerTask = new TimerTask {
                    def run = {
                      val runNow = atomic { implicit tx =>
                        if (throttledState.contains(execution)) {
                          throttledState -= execution
                          addExecution2RunningState(execution, promise)
                          true
                        } else {
                          false
                        }
                      }
                      if (runNow) unsafeDoRun(execution, promise)
                    }
                  }
                  timer.schedule(timerTask, java.util.Date.from(launchDate.atZone(ZoneId.systemDefault()).toInstant))
                  execution.onCancel { () =>
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
            }
            (execution, promise.future)
        }
      ))
  }

  /** Run the [[SideEffect]] function of the provided [[Job]] for the given [[SchedulingContext]].
    *
    * @param job The [[Job]] to run (actually its [[SideEffect]] function)
    * @param context The [[SchedulingContext]] (input of the [[SideEffect]] function)
    * @return The created [[Execution]] along with the `Future` tracking the execution status. */
  def run(job: Job[S], context: S#Context): (Execution[S], Future[Completed]) =
    run0(Seq(job -> context)).head

  /** Run the [[SideEffect]] function of the provided ([[Job Jobs]], [[SchedulingContext SchedulingContexts]]).
    *
    * @param all The [[Job Jobs]] and [[SchedulingContext SchedulingContexts]] to run.
    * @return The created [[Execution Executions]] along with their `Future` tracking the execution status. */
  def runAll(all: Seq[(Job[S], S#Context)]): Seq[(Execution[S], Future[Completed])] =
    run0(all.sortBy(_._2))

  /**
    * Atomically get executor stats.
    * @param jobs the list of jobs ids
    * @return how much ((running, waiting), paused, failing) jobs are in concrete states
    * */
  private def getStateAtomic(jobs: Set[String]) = atomic { implicit txn =>
    (runningExecutionsSizes(jobs), pausedExecutionsSize(jobs), failingExecutionsSize(jobs))
  }

  private[cuttle] def getStats(jobs: Set[String]): IO[Json] = {
    val ((running, waiting), paused, failing) = getStateAtomic(jobs)
    // DB state call
    archivedExecutionsSize(jobs).map(
      finished =>
        Map(
          "running" -> running,
          "waiting" -> waiting,
          "paused" -> paused,
          "failing" -> failing,
          "finished" -> finished
        ).asJson)
  }

  private[cuttle] def healthCheck(): Try[Boolean] =
    Try(queries.healthCheck.transact(xa).unsafeRunSync)

  override def getMetrics(jobs: Set[String], workflow: Workflow[S]): Seq[Metric] = {
    val ((running, waiting), paused, failing) = getStateAtomic(jobs)

    Seq(
      Gauge("cuttle_scheduler_stat_count", "The number of jobs that we have in concrete states")
        .labeled("type" -> "running", running)
        .labeled("type" -> "waiting", waiting)
        .labeled("type" -> "paused", paused)
        .labeled("type" -> "failing", failing))
  }

  override def getMetricsByTag(jobs: Set[String]): Seq[Metric] = atomic { implicit txn =>
    val (running, waiting) = runningExecutions
      .flatMap {
        case (exec, status) =>
          exec.job.tags.map(_.name -> status)
      }
      .partition(_._2 == ExecutionStatus.ExecutionRunning)
    Seq(
      (
        running.groupBy(_._1).mapValues("running" -> _.size).toList ++
          waiting.groupBy(_._1).mapValues("waiting" -> _.size).toList ++
          pausedState.values
            .flatMap(_.keys.flatMap(_.job.tags.map(_.name)))
            .groupBy(identity)
            .mapValues("paused" -> _.size)
            .toList ++
          allFailingExecutions
            .flatMap(_.job.tags.map(_.name))
            .groupBy(identity)
            .mapValues("failing" -> _.size)
            .toList
      ).foldLeft(
        Gauge("cuttle_scheduler_stat_count_by_tag", "The number of jobs that we have in concrete states by tag")
      ) {
        case (gauge, (tag, (status, count))) =>
          gauge.labeled(Set("tag" -> tag, "type" -> status), count)
      })
  }
}
