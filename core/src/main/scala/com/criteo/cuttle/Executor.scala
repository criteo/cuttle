package com.criteo.cuttle

import java.io.{PrintWriter, StringWriter}
import java.time.{Duration, Instant, ZoneId}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.{Timer, TimerTask}

import scala.concurrent.duration._
import scala.concurrent.stm._
import scala.concurrent.{Future, Promise}
import scala.reflect.{classTag, ClassTag}
import scala.util._

import cats.Eq
import cats.effect.IO
import cats.implicits._
import doobie.implicits._
import doobie.util.fragment.Fragment
import io.circe._
import io.circe.java8.time._
import io.circe.syntax._
import lol.http.PartialService

import com.criteo.cuttle.Auth._
import com.criteo.cuttle.ExecutionStatus._
import com.criteo.cuttle.ThreadPools.{SideEffectThreadPool, _}
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

  /** Simple retry strategy. Retry after the constant retry window.
    * @param duration Duration of a retry window.
    */
  def SimpleRetryStategy(duration: scala.concurrent.duration.Duration) = new RetryStrategy {
    def retryWindow: Duration = Duration.ofNanos(duration.toNanos)
    def apply[S <: Scheduling](job: Job[S], ctx: S#Context, previouslyFailing: List[String]): Duration = retryWindow
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

  implicit lazy val executionLogEncoder: Encoder[ExecutionLog] = new Encoder[ExecutionLog] {
    override def apply(execution: ExecutionLog) = Json.obj(
      "id" -> execution.id.asJson,
      "job" -> execution.job.asJson,
      "startTime" -> execution.startTime.asJson,
      "endTime" -> execution.endTime.asJson,
      "context" -> execution.context,
      "status" -> (execution.status match {
        case ExecutionSuccessful => "successful"
        case ExecutionFailed     => "failed"
        case ExecutionRunning    => "running"
        case ExecutionWaiting    => "waiting"
        case ExecutionPaused     => "paused"
        case ExecutionThrottled  => "throttled"
        case ExecutionTodo       => "todo"
      }).asJson,
      "failing" -> execution.failing.map {
        case FailingJob(failedExecutions, nextRetry) =>
          Json.obj(
            "failedExecutions" -> Json.fromValues(failedExecutions.map(_.asJson(executionLogEncoder))),
            "nextRetry" -> nextRetry.asJson
          )
      }.asJson,
      "waitingSeconds" -> execution.waitingSeconds.asJson
    )
  }
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
  projectName: String,
  projectVersion: String
)(implicit val executionContext: SideEffectThreadPool) {

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
      context.asJson,
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
  def run(): Future[Completed] =
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

private[cuttle] object Executor {

  // we save a mapping of ThreadName -> ExecutionStreams to be able to redirect logs comming
  // form different SideEffect (Futures) to the corresponding ExecutionStreams
  private val threadNamesToStreams = new ConcurrentHashMap[String, ExecutionStreams]

  def getStreams(threadName: String): Option[ExecutionStreams] = Option(threadNamesToStreams.get(threadName))
}

/** An [[Executor]] is responsible to actually execute the [[SideEffect]] functions for the
  * given [[Execution Executions]]. */
class Executor[S <: Scheduling] private[cuttle] (val platforms: Seq[ExecutionPlatform],
                                                 xa: XA,
                                                 logger: Logger,
                                                 val projectName: String,
                                                 val projectVersion: String)(implicit retryStrategy: RetryStrategy)
    extends MetricProvider[S] {

  import ExecutionStatus._
  import Implicits.sideEffectThreadPool

  private implicit val contextOrdering: Ordering[S#Context] = Ordering.by(c => c: SchedulingContext)

  private val queries = new Queries {
    val appLogger: Logger = logger
  }

  // TODO: move to the scheduler
  private[cuttle] val runningState = TMap.empty[Execution[S], Future[Completed]]
  private[cuttle] val throttledState = TMap.empty[Execution[S], (Promise[Completed], FailingJob)]
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
      val waitingExecutions = platforms.flatMap(_.waiting).toSet

      recentFailures
        .flatMap({
          case ((job, context), (_, failingJob)) =>
            runningIds
              .get((job.id, context))
              .map(execution => {
                val status =
                  if (execution.isWaiting.get || waitingExecutions.contains(execution)) ExecutionWaiting
                  else ExecutionRunning
                (execution, failingJob, status)
              })
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
    val intervalSeconds = 1
    utils.awakeEvery(intervalSeconds.seconds).map(_ => {
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
          case "status" =>
            _.sortBy({ case (_, _, status) => status.toString })
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

  private[cuttle] def cancelExecution(executionId: String)(implicit user: User): Unit = {
    val toCancel = atomic { implicit tx =>
      (runningState.keys ++ throttledState.keys)
        .find(_.id == executionId)
    }
    toCancel.foreach(_.cancel())
  }

  private[cuttle] def getExecution(queryContexts: Fragment, executionId: String): IO[Option[ExecutionLog]] =
    atomic { implicit tx =>
      val predicate = (e: Execution[S]) => e.id == executionId
      throttledState.keys
        .find(predicate)
        .map(e => e.toExecutionLog(ExecutionThrottled).copy(failing = throttledState.get(e).map(_._2)))
        .orElse(runningState.keys.find(predicate).map { execution =>
          execution.toExecutionLog(flagWaitingExecutions(execution :: Nil).head._2)
        })
    } match {
      case None                      => queries.getExecutionById(queryContexts, executionId).transact(xa)
      case (a: Option[ExecutionLog]) => IO.pure(a)
    }

  private[cuttle] def openStreams(executionId: String): fs2.Stream[IO, Byte] =
    ExecutionStreams.getStreams(executionId, queries, xa)

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
      (runningState.keys ++ throttledState.keys)
        .find(execution => execution.id == executionId)
    }
    toForce.foreach(_.forceSuccess())
  }

  private[cuttle] def gracefulShutdown(timeout: scala.concurrent.duration.Duration)(implicit user: User): Unit = {

    import utils._

    // cancel all executions
    val toCancel = atomic { implicit txn =>
      isShuttingDown() = true
      runningState.keys ++ throttledState.keys
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
                  execution.context.logIntoDatabase
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
    case class Throttled(launchDate: Instant) extends NewExecution

    val index: Map[(Job[S], S#Context), (Execution[S], Future[Completed])] = runningState.single.map {
      case (execution, future) =>
        ((execution.job, execution.context), (execution, future))
    }.toMap
    val existingOrNew
      : Seq[Either[(Execution[S], Future[Completed]), (Job[S], Execution[S], Promise[Completed], NewExecution)]] =
      atomic { implicit txn =>
        if (isShuttingDown()) {
          Seq.empty
        } else
          all.distinct.zipWithIndex.map {
            case ((job, context), i) =>
              if (i > 1000 && i % 1000 == 0) logger.info(s"Submitted ${i}/${all.size} jobs")
              val maybeAlreadyRunning: Option[(Execution[S], Future[Completed])] = index.get((job, context))

              lazy val maybeThrottled: Option[(Execution[S], Future[Completed])] =
                throttledState.find { case (e, _) => e.job == job && e.context == context }.map {
                  case (e, (p, _)) => (e, p.future)
                }

              maybeAlreadyRunning
                .orElse(maybeThrottled)
                .toLeft {
                  val nextExecutionId = utils.randomUUID

                  val streams = new ExecutionStreams {
                    def writeln(str: CharSequence) = {
                      ExecutionStreams.writeln(nextExecutionId, str)
                      logger.debug(s"[$nextExecutionId] $str")
                    }
                  }

                  // wrap the execution context so that we can register the name of the thread of each
                  // runnable (and thus future) that will be run by the side effect.
                  val sideEffectExecutionContext = SideEffectThreadPool.wrap(runnable =>
                    new Runnable {
                      override def run(): Unit = {
                        val tName = Thread.currentThread().getName
                        Executor.threadNamesToStreams.put(tName, streams)
                        try {
                          runnable.run()
                        } finally {
                          Executor.threadNamesToStreams.remove(tName)
                        }
                      }
                  })(Implicits.sideEffectThreadPool)

                  val execution = Execution(
                    id = nextExecutionId,
                    job,
                    context,
                    streams = streams,
                    platforms,
                    projectName,
                    projectVersion
                  )(sideEffectExecutionContext)
                  val promise = Promise[Completed]

                  if (recentFailures.contains(job -> context)) {
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
          case (_, execution, promise, whatToDo) =>
            Future {
              execution.streams.debug(s"Execution: ${execution.id}")
              execution.streams.debug(s"Context: ${execution.context.asJson}")
              execution.streams.debug()
              whatToDo match {
                case ToRunNow =>
                  unsafeDoRun(execution, promise)
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
    (runningExecutionsSizes(jobs), failingExecutionsSize(jobs))
  }

  private[cuttle] def getStats(jobs: Set[String]): IO[Json] = {
    val ((running, waiting), failing) = getStateAtomic(jobs)
    // DB state call
    archivedExecutionsSize(jobs).map(
      finished =>
        Map(
          "running" -> running,
          "waiting" -> waiting,
          // TODO: to check in UI
          "paused" -> 0,
          "failing" -> failing,
          "finished" -> finished
        ).asJson)
  }

  private[cuttle] def healthCheck(): Try[Boolean] =
    Try(queries.healthCheck.transact(xa).unsafeRunSync)

  private case class ExecutionInfo(jobId: String, tags: Set[String], status: ExecutionStatus)

  /**
    * @param jobs list of jobs whose metrics will be retrieved
    * @param getStateAtomic Atomically get executor stats. Given a list of jobs ids, returns how much
    *                       ((running, waiting), paused, failing) jobs are in concrete states
    * @param runningExecutions executions which are either either running or waiting for a free thread to start
    *                          @param pausedExecutions
    */
  private[cuttle] def getMetrics(jobs: Set[Job[S]])(
    getStateAtomic: Set[String] => ((Int, Int), Int),
    runningExecutions: Seq[(Execution[S], ExecutionStatus)],
    failingExecutions: Seq[Execution[S]]
  ): Seq[Metric] = {
    val jobIds = jobs.map(_.id)

    val ((runningCount, waitingCount), failingCount) = getStateAtomic(jobIds)
    val statMetrics = Seq(
      Gauge("cuttle_scheduler_stat_count", "The number of jobs that we have in concrete states")
        .labeled("type" -> "running", runningCount)
        .labeled("type" -> "waiting", waitingCount)
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

    val failing: Seq[ExecutionInfo] = failingExecutions.map { exec =>
      ExecutionInfo(exec.job.id, exec.job.tags.map(_.name), ExecutionStatus.ExecutionThrottled)
    }

    statMetrics ++
      Seq(getMetricsByTag(running, waiting, failing)) ++
      Seq(getMetricsByJob(running, waiting, failing)) ++
      Seq(
        executionsCounters
          .single()
          .withDefaultsFor({
            for {
              job <- jobs.toSeq
              outcome <- Seq("success", "failure")
            } yield
              Set(("job_id", job.id), ("type", outcome)) ++ (if (job.tags.nonEmpty)
                                                               Set("tags" -> job.tags.map(_.name).mkString(","))
                                                             else Nil)
          }))
  }

  /**
    * @param jobs the list of jobs ids
    */
  override def getMetrics(jobIds: Set[String], jobs: Workload[S]): Seq[Metric] =
    atomic { implicit txn =>
      getMetrics(jobs.all.filter(j => jobIds.contains(j.id)))(
        getStateAtomic,
        runningExecutions,
        allFailingExecutions
      )
    }

  private def getMetricsByTag(running: Seq[ExecutionInfo],
                              waiting: Seq[ExecutionInfo],
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
                              failing: Seq[ExecutionInfo]): Metrics.Metric =
    (
      running.groupBy(_.jobId).mapValues("running" -> _.size).toList ++
        waiting.groupBy(_.jobId).mapValues("waiting" -> _.size).toList ++
        failing.groupBy(_.jobId).mapValues("failing" -> _.size).toList
    ).foldLeft(
      Gauge("cuttle_scheduler_stat_count_by_job", "The number of executions that we have in concrete states by job")
    ) {
      case (gauge, (jobId, (status, count))) =>
        gauge.labeled(Set("job" -> jobId, "type" -> status), count)
    }
}
