package com.criteo.cuttle

import java.io.{PrintWriter, StringWriter}
import java.time.{Duration, Instant, ZoneId}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.{Timer, TimerTask}

import scala.concurrent.duration._
import scala.concurrent.stm.Txn.ExternalDecider
import scala.concurrent.stm._
import scala.concurrent.{Future, Promise}
import scala.reflect.{classTag, ClassTag}
import scala.util.{Failure, Success, Try}
import cats.Eq
import cats.effect.IO
import cats.implicits._
import doobie.implicits._
import doobie.util.fragment.Fragment
import io.circe._
import io.circe.syntax._
import lol.http.PartialService
import com.criteo.cuttle.Auth._
import com.criteo.cuttle.ExecutionContexts.{SideEffectExecutionContext, _}
import com.criteo.cuttle.Metrics._
import com.criteo.cuttle.platforms.ExecutionPool

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
  def unsubscribeOn[A](f: Future[A]) = f.andThen { case _ => unsubscribe() }(execution.executionContext)
}

private[cuttle] case class PausedJobWithExecutions[S <: Scheduling](id: String,
                                                                    user: User,
                                                                    date: Instant,
                                                                    executions: Map[Execution[S], Promise[Completed]]) {
  def toPausedJob(): PausedJob = PausedJob(id, user, date)
}

private[cuttle] case class PausedJob(id: String, user: User, date: Instant) {
  def toPausedJobWithExecutions[S <: Scheduling](): PausedJobWithExecutions[S] =
    PausedJobWithExecutions(id, user, date, Map.empty[Execution[S], Promise[Completed]])
}

//private[cuttle] object PausedJob {
//  import io.circe.generic.semiauto._
//  import io.circe._
//
//  implicit val encoder: Encoder[PausedJob] = deriveEncoder
//}

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
  projectName: String
)(implicit val executionContext: SideEffectExecutionContext) {

  private var waitingSeconds = 0
  private[cuttle] var startTime: Option[Instant] = None
  private val cancelListeners = TSet.empty[CancellationListener]
  private val cancelled = Ref(false)

  /**
    * An execution with forcedSuccess set to true will have its side effect return a successful Future instance even if the
    * user code raised an exception or returned a failed Future instance.
    */
  private[cuttle] val forcedSuccess = Ref(false)

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

  def forceSuccess()(implicit user: User): Unit =
    if (!atomic { implicit txn =>
          forcedSuccess.getAndTransform(_ => true)
        }) {
      streams.debug(
        s"""Possible execution failures will be ignored and final execution status will be marked as success.
                       |Change initiated by user ${user.userId} at ${Instant.now().toString}.""".stripMargin)
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

  /** Allows up to `concurrencyLimit` concurrent executions of a code block over a lock. If
    * several[[SideEffect]] functions need to race for a shared thread unsafe resource,
    * they can use this helper function to ensure that at most `concurrencyLimit` code
    * blocks will run at once. Think about it as an asynchronous `Semaphore` helper.
    *
    * While waiting for the lock, the [[Execution]] will be seen as __WAITING__ in the UI and the API.
    */
  def withMaxParallelRuns[A, B](lock: A, concurrencyLimit: Int)(thunk: => Future[B]): Future[B] =
    if (isWaiting.get) {
      sys.error(s"Already waiting")
    } else {
      streams.debug(s"Execution waiting for lock ${lock}...")
      isWaiting.set(true)
      val pool = atomic { implicit tx =>
        if (!Execution.locks.contains(lock)) {
          Execution.locks.update(lock, new ExecutionPool(concurrencyLimit))
        }
        Execution.locks(lock)
      }
      require(
        pool.concurrencyLimit == concurrencyLimit,
        s"Inconsistent concurrency limits defined for the execution pool for $lock"
      )
      pool.run(this, s"Locked on $lock") { () =>
        streams.debug(s"Resuming")
        isWaiting.set(false)
        thunk
      }
    }

  /** Synchronize a code block over a lock. If several [[SideEffect]] functions need to race
    * for a shared thread unsafe resource, they can use this helper function to ensure that only
    * one code block will run at once. Think about it as an asynchronous `synchronized` helper.
    *
    * While waiting for the lock, the [[Execution]] will be seen as __WAITING__ in the UI and the API.
    */
  def synchronize[A, B](lock: A)(thunk: => Future[B]): Future[B] =
    withMaxParallelRuns(lock, concurrencyLimit = 1)(thunk)

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
  import Implicits.sideEffectExecutionContext

  private implicit val contextOrdering: Ordering[S#Context] = Ordering.by(c => c: SchedulingContext)

  private val queries = new Queries {
    val appLogger: Logger = logger
  }

  private val pausedState: TMap[String, PausedJobWithExecutions[S]] = {
    val pausedJobs = queries.getPausedJobs.transact(xa).unsafeRunSync
    TMap(pausedJobs.map(pausedJob => pausedJob.id -> pausedJob.toPausedJobWithExecutions[S]()): _*)
  }
  private val runningState = TMap.empty[Execution[S], Future[Completed]]
  private val throttledState = TMap.empty[Execution[S], (Promise[Completed], FailingJob)]
  private val recentFailures = TMap.empty[(Job[S], S#Context), (Option[Execution[S]], FailingJob)]

  // signals whether the instance is shutting down
  private val isShuttingDown: Ref[Boolean] = Ref(false)
  private val timer = new Timer("com.criteo.cuttle.Executor.timer")
  private val executionsCounters: Ref[Counter[Long]] = Ref(
    Counter[Long](
      "cuttle_executions_total",
      help = "The number of finished executions that we have in concrete states by job and by tag"
    ))

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
      .compile
      .drain
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
    pausedState.single.values.foldLeft(0) {
      case (acc, PausedJobWithExecutions(id, _, _, executions)) if filteredJobs.contains(id) =>
        acc + executions.size
    }

  private[cuttle] def pausedExecutions(filteredJobs: Set[String],
                                       sort: String,
                                       asc: Boolean,
                                       offset: Int,
                                       limit: Int): Seq[ExecutionLog] = {
    val filteredExecutions = pausedState.single.values
      .collect {
        case PausedJobWithExecutions(id, _, _, executions) if filteredJobs.contains(id) => executions.keys
      }
      .flatten
      .toSeq

    val ordering = sort match {
      case "job"       => Ordering.by((e: Execution[S]) => (e.job.id, e))
      case "startTime" => Ordering.by((e: Execution[S]) => (e.startTime.toString, e))
      case _           => Ordering[Execution[S]]
    }

    val finalOrdering = if (asc) ordering else ordering.reverse

    val sortedExecutions = filteredExecutions.sorted(finalOrdering)

    sortedExecutions
      .slice(offset, offset + limit)
      .map(_.toExecutionLog(ExecutionPaused))
  }

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

  private[cuttle] def pausedJobs: Seq[PausedJob] = pausedState.single.values.map(_.toPausedJob()).toSeq

  private[cuttle] def cancelExecution(executionId: String)(implicit user: User): Unit = {
    val toCancel = atomic { implicit tx =>
      (runningState.keys ++ pausedState.values.flatMap(_.executions.keys) ++ throttledState.keys)
        .find(_.id == executionId)
    }
    toCancel.foreach(_.cancel())
  }

  private[cuttle] def getExecution(queryContexts: Fragment, executionId: String): IO[Option[ExecutionLog]] =
    atomic { implicit tx =>
      val predicate = (e: Execution[S]) => e.id == executionId
      pausedState.values
        .flatMap(_.executions.keys)
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
    val pauseDate = Instant.now()
    val pausedJobs = jobs.map(job => PausedJob(job.id, user, pauseDate))
    val pauseQuery = pausedJobs.map(queries.pauseJob).reduceLeft(_ *> _)

    val executionsToCancel = atomic { implicit tx =>
      Txn.setExternalDecider(new ExternalDecider {
        def shouldCommit(implicit txn: InTxnEnd): Boolean = {
          pauseQuery.transact(xa).unsafeRunSync
          true
        }
      })

      pausedJobs.flatMap { pausedJob =>
        pausedState.getOrElseUpdate(pausedJob.id, pausedJob.toPausedJobWithExecutions())
        runningState.filterKeys(_.job.id == pausedJob.id).keys ++ throttledState
          .filterKeys(_.job.id == pausedJob.id)
          .keys
      }
    }
    logger.debug(s"we will cancel ${executionsToCancel.size} executions")
    executionsToCancel.toList.sortBy(_.context).reverse.foreach { execution =>
      execution.streams.debug(s"Job has been paused by user ${user.userId}")
      execution.cancel()
    }
  }

  private[cuttle] def resumeJobs(jobs: Set[Job[S]])(implicit user: User): Unit = {
    val resumeQuery = jobs.map(job => queries.resumeJob(job.id)).reduceLeft(_ *> _)
    val jobIdsToResume = jobs.map(_.id)

    val executionsToResume = atomic { implicit tx =>
      Txn.setExternalDecider(new ExternalDecider {
        def shouldCommit(implicit tx: InTxnEnd): Boolean = {
          resumeQuery.transact(xa).unsafeRunSync
          true
        }
      })
      val executionsToResume = pausedState.collect {
        case (id, PausedJobWithExecutions(_, _, _, executions)) if jobIdsToResume.contains(id) => executions
      }.flatten

      jobs.foreach(job => pausedState -= job.id)

      executionsToResume.map {
        case (execution, promise) =>
          addExecution2RunningState(execution, promise)
          execution -> promise
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

  private[cuttle] def forceSuccess(executionId: String)(implicit user: User): Unit = {
    val toForce = atomic { implicit tx =>
      (runningState.keys ++ pausedState.values.flatMap(_.executions.keys) ++ throttledState.keys)
        .find(execution => execution.id == executionId)
    }
    toForce.foreach(_.forceSuccess())
  }

  private[cuttle] def gracefulShutdown(timeout: scala.concurrent.duration.Duration)(implicit user: User): Unit = {

    import utils._

    // cancel all executions
    val toCancel = atomic { implicit txn =>
      isShuttingDown() = true
      runningState.keys ++ pausedState.values.flatMap(_.executions.keys) ++ throttledState.keys
    }
    toCancel.foreach(_.cancel())

    val runningFutures = atomic { implicit tx =>
      runningState.map({ case (_, v) => v })
    }

    Future
      .firstCompletedOf(List(Timeout(timeout), Future.sequence(runningFutures)))
      .andThen {
        case _ => hardShutdown()
      }
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
        case Failure(e) => if (execution.forcedSuccess.single()) Future.successful(Completed) else Future.failed(e)
      }).andThen {
          case Success(_) =>
            execution.streams.debug(s"Execution successful")
            atomic { implicit txn =>
              runningState -= execution
              recentFailures -= (execution.job -> execution.context)
              updateFinishedExecutionCounters(execution, "success")
            }
          case Failure(e) =>
            val stacktrace = new StringWriter()
            e.printStackTrace(new PrintWriter(stacktrace))
            execution.streams.error(s"Execution failed:")
            execution.streams.error(stacktrace.toString)
            atomic {
              implicit tx =>
                updateFinishedExecutionCounters(execution, "failure")
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

  private[cuttle] def updateFinishedExecutionCounters(execution: Execution[S], status: String): Unit =
    atomic { implicit txn =>
      val tagsLabel =
        if (execution.job.tags.nonEmpty)
          Set("tags" -> execution.job.tags.map(_.name).mkString(","))
        else
          Set.empty
      executionsCounters() = executionsCounters().inc(
        Set("type" -> status, "job_id" -> execution.job.id) ++ tagsLabel
      )
    }
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

              lazy val maybePaused: Option[(Execution[S], Future[Completed])] = pausedState
                .get(job.id)
                .flatMap(_.executions.collectFirst {
                  case (execution, promise) if execution.context == context =>
                    execution -> promise.future
                })

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
                    job,
                    context,
                    streams = new ExecutionStreams {
                      def writeln(str: CharSequence) = ExecutionStreams.writeln(nextExecutionId, str)
                    },
                    platforms,
                    projectName
                  )
                  val promise = Promise[Completed]

                  if (pausedState.contains(job.id)) {
                    val pausedJobWithExecutions = pausedState(job.id)
                    pausedState += job.id -> pausedJobWithExecutions.copy(
                      executions = pausedJobWithExecutions.executions + (execution -> promise))
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
                  // we attach this callback to freshly created "Paused" execution
                  execution.onCancel {
                    () =>
                      val cancelNow = atomic { implicit tx =>
                        // we take the first pair of jobId, executions and filter executions by execution
                        val maybeJobId2Executions = pausedState.get(job.id).collectFirst {
                          case pwe @ PausedJobWithExecutions(_, _, _, executions) if executions.contains(execution) =>
                            job.id -> pwe.copy(executions = executions.filterKeys(_ == execution))
                        }

                        maybeJobId2Executions.fold(false) { x =>
                          pausedState += x
                          true
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

  private case class ExecutionInfo(jobId: String, tags: Set[String], status: ExecutionStatus)

  /**
    * @param jobs list of job IDs
    * @param getStateAtomic Atomically get executor stats. Given a list of jobs ids, returns how much
    *                       ((running, waiting), paused, failing) jobs are in concrete states
    * @param runningExecutions executions which are either either running or waiting for a free thread to start
    *                          @param pausedExecutions
    */
  private[cuttle] def getMetrics(jobs: Set[String])(
    getStateAtomic: Set[String] => ((Int, Int), Int, Int),
    runningExecutions: Seq[(Execution[S], ExecutionStatus)],
    pausedExecutions: Seq[Execution[S]],
    failingExecutions: Seq[Execution[S]]
  ): Seq[Metric] = {
    val ((runningCount, waitingCount), pausedCount, failingCount) = getStateAtomic(jobs)
    val statMetrics = Seq(
      Gauge("cuttle_scheduler_stat_count", "The number of jobs that we have in concrete states")
        .labeled("type" -> "running", runningCount)
        .labeled("type" -> "waiting", waitingCount)
        .labeled("type" -> "paused", pausedCount)
        .labeled("type" -> "failing", failingCount)
    )

    val (running: Seq[ExecutionInfo], waiting: Seq[ExecutionInfo]) = runningExecutions
      .map {
        case (exec, status) =>
          ExecutionInfo(exec.job.id, exec.job.tags.map(_.name), status)
      }
      .partition { execution =>
        execution.status == ExecutionStatus.ExecutionRunning
      }

    val paused: Seq[ExecutionInfo] = pausedExecutions.map { exec =>
      ExecutionInfo(exec.job.id, exec.job.tags.map(_.name), ExecutionStatus.ExecutionPaused)
    }

    val failing: Seq[ExecutionInfo] = failingExecutions.map { exec =>
      ExecutionInfo(exec.job.id, exec.job.tags.map(_.name), ExecutionStatus.ExecutionThrottled)
    }

    statMetrics ++
      Seq(getMetricsByTag(running, waiting, paused, failing)) ++
      Seq(getMetricsByJob(running, waiting, paused, failing)) ++
      Seq(executionsCounters.single())
  }

  /**
    * @param jobs the list of jobs ids
    */
  override def getMetrics(jobs: Set[String], workflow: Workflow[S]): Seq[Metric] =
    atomic { implicit txn =>
      getMetrics(jobs)(
        getStateAtomic,
        runningExecutions,
        pausedState.values.flatMap(_.executions.keys).toSeq,
        allFailingExecutions
      )
    }

  private def getMetricsByTag(running: Seq[ExecutionInfo],
                              waiting: Seq[ExecutionInfo],
                              paused: Seq[ExecutionInfo],
                              failing: Seq[ExecutionInfo]): Metrics.Metric =
    (// Explode by tag
    running
      .flatMap { info =>
        info.tags
      }
      .groupBy(identity)
      .mapValues("running" -> _.size)
      .toList ++
      waiting
        .flatMap { info =>
          info.tags
        }
        .groupBy(identity)
        .mapValues("waiting" -> _.size)
        .toList ++
      paused
        .flatMap { info =>
          info.tags
        }
        .groupBy(identity)
        .mapValues("paused" -> _.size)
        .toList ++
      failing
        .flatMap { info =>
          info.tags
        }
        .groupBy(identity)
        .mapValues("failing" -> _.size)
        .toList).foldLeft(
      Gauge("cuttle_scheduler_stat_count_by_tag", "The number of executions that we have in concrete states by tag")
    ) {
      case (gauge, (tag, (status, count))) =>
        gauge.labeled(Set("tag" -> tag, "type" -> status), count)
      case (gauge, _) =>
        gauge
    }

  private def getMetricsByJob(running: Seq[ExecutionInfo],
                              waiting: Seq[ExecutionInfo],
                              paused: Seq[ExecutionInfo],
                              failing: Seq[ExecutionInfo]): Metrics.Metric =
    (
      running.groupBy(_.jobId).mapValues("running" -> _.size).toList ++
        waiting.groupBy(_.jobId).mapValues("waiting" -> _.size).toList ++
        paused.groupBy(_.jobId).mapValues("paused" -> _.size).toList ++
        failing.groupBy(_.jobId).mapValues("failing" -> _.size).toList
    ).foldLeft(
      Gauge("cuttle_scheduler_stat_count_by_job", "The number of executions that we have in concrete states by job")
    ) {
      case (gauge, (jobId, (status, count))) =>
        gauge.labeled(Set("job" -> jobId, "type" -> status), count)
    }
}
