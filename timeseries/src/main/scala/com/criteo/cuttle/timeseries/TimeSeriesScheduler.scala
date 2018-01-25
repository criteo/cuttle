package com.criteo.cuttle.timeseries

import java.time.ZoneOffset.UTC
import java.time._
import java.time.temporal.ChronoUnit._
import java.time.temporal.{ChronoUnit, TemporalAdjusters}
import java.util.UUID

import scala.concurrent._
import scala.concurrent.duration.{Duration => ScalaDuration}
import scala.concurrent.stm._

import cats.Eq
import cats.effect.IO
import cats.mtl.implicits._
import doobie._
import doobie.implicits._
import io.circe._
import io.circe.generic.semiauto._
import io.circe.syntax._

import com.criteo.cuttle.ExecutionContexts.Implicits.sideEffectExecutionContext
import com.criteo.cuttle.ExecutionContexts._
import com.criteo.cuttle.Metrics._
import com.criteo.cuttle._
import com.criteo.cuttle.timeseries.Internal._
import com.criteo.cuttle.timeseries.TimeSeriesCalendar.{Daily, Hourly, Monthly, Weekly}
import com.criteo.cuttle.timeseries.intervals.Bound.{Bottom, Finite, Top}
import com.criteo.cuttle.timeseries.intervals.{Bound, Interval, IntervalMap}

/** Represents calendar partitions for which a job will be run by the [[TimeSeriesScheduler]].
  * See the companion object for the available calendars. */
sealed trait TimeSeriesCalendar {
  private[timeseries] def next(t: Instant): Instant
  private[timeseries] def truncate(t: Instant): Instant
  private[timeseries] def ceil(t: Instant): Instant = {
    val truncated = truncate(t)
    if (truncated == t) t
    else next(t)
  }
  private[timeseries] def inInterval(interval: Interval[Instant], maxPeriods: Int) = {
    def go(lo: Instant, hi: Instant): List[(Instant, Instant)] = {
      val nextLo = next(lo)
      if (nextLo.isAfter(hi)) List.empty
      else ((lo, nextLo) +: go(nextLo, hi))
    }
    interval match {
      case Interval(Finite(lo), Finite(hi)) =>
        go(ceil(lo), hi).grouped(maxPeriods).map(xs => (xs.head._1, xs.last._2))
      case _ =>
        sys.error("panic")
    }
  }
  private[timeseries] def split(interval: Interval[Instant]) = {
    def go(lo: Instant, hi: Instant): List[(Instant, Instant)] = {
      val nextLo = next(lo)
      if (nextLo.isBefore(hi)) ((lo, nextLo) +: go(nextLo, hi))
      else List((lo, hi))
    }
    interval match {
      case Interval(Finite(lo), Finite(hi)) =>
        go(lo, hi)
      case _ =>
        sys.error("panic")
    }
  }
}

private[timeseries] sealed trait TimeSeriesCalendarView {
  def calendar: TimeSeriesCalendar
  def upper(): TimeSeriesCalendarView
  val aggregationFactor: Int
}

/** Define the available calendars. */
object TimeSeriesCalendar {

  /** An hourly calendar. Hours are defined as complete calendar hours starting
    * at 00 minutes, 00 seconds. */
  case object Hourly extends TimeSeriesCalendar {
    def truncate(t: Instant) = t.truncatedTo(HOURS)
    def next(t: Instant) =
      t.truncatedTo(HOURS).plus(1, HOURS)
  }

  /** An daily calendar. Days are defined as complete calendar days starting a midnight and
    * during 24 hours. If the specified timezone defines lightsaving it is possible that some
    * days are 23 or 25 horus thus.
    *
    * @param tz The time zone for which these _days_ are defined.
    */
  case class Daily(tz: ZoneId) extends TimeSeriesCalendar {
    def truncate(t: Instant) = t.atZone(tz).truncatedTo(DAYS).toInstant
    def next(t: Instant) = t.atZone(tz).truncatedTo(DAYS).plus(1, DAYS).toInstant
  }

  /** A weekly calendar. Weeks are defined as complete calendar weeks starting on a specified day of week
    * at midnight and lasting 7 days. The specified time zone is used to define the exact week start instant.
    *
    * @param tz The time zone for which these _weeks_ are defined.
    */
  case class Weekly(tz: ZoneId, startDay: DayOfWeek) extends TimeSeriesCalendar {
    private def truncateToWeek(t: ZonedDateTime) = t.`with`(TemporalAdjusters.previous(startDay)).truncatedTo(DAYS)
    def truncate(t: Instant) = truncateToWeek(t.atZone(tz)).toInstant
    def next(t: Instant) = truncateToWeek(t.atZone(tz).plus(1, WEEKS)).toInstant
  }

  /** An monthly calendar. Months are defined as complete calendar months starting on the 1st day and
    * during 28,29,30 or 31 days. The specified time zone is used to define the exact month start instant.
    *
    * @param tz The time zone for which these _months_ are defined.
    */
  case class Monthly(tz: ZoneId) extends TimeSeriesCalendar {
    private val truncateToMonth = (t: ZonedDateTime) =>
      t.`with`(TemporalAdjusters.firstDayOfMonth()).truncatedTo(ChronoUnit.DAYS)
    def truncate(t: Instant) = truncateToMonth(t.atZone(tz)).toInstant
    def next(t: Instant) = truncateToMonth(t.atZone(tz)).plus(1, MONTHS).toInstant
  }

  private[timeseries] implicit val calendarEncoder = new Encoder[TimeSeriesCalendar] {
    override def apply(calendar: TimeSeriesCalendar) = calendar match {
      case Hourly => Json.obj("period" -> "hourly".asJson)
      case Daily(tz: ZoneId) =>
        Json.obj(
          "period" -> "daily".asJson,
          "zoneId" -> tz.getId().asJson
        )
      case Weekly(tz: ZoneId, startDay: DayOfWeek) =>
        Json.obj(
          "period" -> "weekly".asJson,
          "zoneId" -> tz.getId.asJson,
          "startDay" -> startDay.toString.asJson
        )
      case Monthly(tz: ZoneId) =>
        Json.obj(
          "period" -> "monthly".asJson,
          "zoneId" -> tz.getId().asJson
        )
    }
  }
}

private[timeseries] object TimeSeriesCalendarView {
  def apply(calendar: TimeSeriesCalendar) = calendar match {
    case TimeSeriesCalendar.Hourly        => new HourlyView(1)
    case TimeSeriesCalendar.Daily(tz)     => new DailyView(tz, 1)
    case TimeSeriesCalendar.Weekly(tz, _) => new WeeklyView(tz, 1)
    case TimeSeriesCalendar.Monthly(tz)   => new MonthlyView(tz, 1)
  }
  sealed trait GenericView extends TimeSeriesCalendarView {
    def over: (Int, TimeSeriesCalendar)
    def calendar = over._2
    def truncate(t: Instant) = calendar.truncate(t)
    def next(t: Instant) = (1 to over._1).foldLeft(calendar.truncate(t))((acc, _) => calendar.next(acc))
    def upper(): TimeSeriesCalendarView
  }
  case class HourlyView(aggregationFactor: Int) extends GenericView {
    def over = (1, Hourly)
    override def upper: TimeSeriesCalendarView = new DailyView(UTC, aggregationFactor * 24)
  }
  case class DailyView(tz: ZoneId, aggregationFactor: Int) extends GenericView {
    def over = (1, Daily(tz))
    override def upper: TimeSeriesCalendarView = new WeeklyView(tz, aggregationFactor * 7)
  }
  case class WeeklyView(tz: ZoneId, aggregationFactor: Int) extends GenericView {
    def over = (7, Daily(tz))
    override def upper: TimeSeriesCalendarView = new MonthlyView(tz, aggregationFactor * 4)
  }
  case class MonthlyView(tz: ZoneId, aggregationFactor: Int) extends GenericView {
    def over = (1, Monthly(tz))
    override def upper: TimeSeriesCalendarView = new MonthlyView(tz, 1)
  }
}

/** A [[Backfill]] allows to recompute already computed time partitions in the past.
  *
  * @param id Unique id for the backfill.
  * @param start Start instant for the partitions to backfill.
  * @param end End instant for the partitions to backfill.
  * @param jobs Indicates the part of the graph to backfill.
  * @param priority The backfill priority. If minus than 0 it is less priority than the day
  *                 to day workload. If more than 0 it becomes more prioritary and can delay
  *                 the day to day workload.
  * @param description Description (for audit logs).
  * @param status Status of the backfill.
  * @param createdBy User who created the backfill (for audit logs).
  */
case class Backfill(id: String,
                    start: Instant,
                    end: Instant,
                    jobs: Set[Job[TimeSeries]],
                    priority: Int,
                    name: String,
                    description: String,
                    status: String,
                    createdBy: String)

private[timeseries] object Backfill {
  implicit val eqInstance: Eq[Backfill] = Eq.fromUniversalEquals[Backfill]
  implicit val encoder: Encoder[Backfill] = deriveEncoder
  implicit def decoder(implicit jobs: Set[Job[TimeSeries]]) =
    deriveDecoder[Backfill]
}

/** A [[TimeSeriesContext]] is passed to [[com.criteo.cuttle.Execution Executions]] initiated by
  * the [[TimeSeriesScheduler]].
  *
  * @param start Start instant of the partition to compute.
  * @param end End instant of the partition to compute.
  * @param backfill If this execution is for a backfill, the [[Backfill]] informations are provided.
  */
case class TimeSeriesContext(start: Instant, end: Instant, backfill: Option[Backfill] = None)
    extends SchedulingContext {

  def toJson: Json = this.asJson

  def log: ConnectionIO[String] = Database.serializeContext(this)

  def toInterval: Interval[Instant] = Interval(start, end)

  def compareTo(other: SchedulingContext) = other match {
    case TimeSeriesContext(otherStart, _, otherBackfill) =>
      val priority: (Option[Backfill] => Int) = _.map(_.priority).getOrElse(0)
      val thisBackfillPriority = priority(backfill)
      val otherBackfillPriority = priority(otherBackfill)
      if (thisBackfillPriority == otherBackfillPriority) {
        start.compareTo(otherStart)
      } else {
        thisBackfillPriority.compareTo(otherBackfillPriority)
      }
  }
}

private[timeseries] object TimeSeriesContext {
  implicit val encoder: Encoder[TimeSeriesContext] = deriveEncoder
  implicit def decoder(implicit jobs: Set[Job[TimeSeries]]): Decoder[TimeSeriesContext] =
    deriveDecoder
}

/** A [[TimeSeriesDependency]] qualify the dependency between 2 [[com.criteo.cuttle.Job Jobs]] in a
  * [[TimeSeries]] [[com.criteo.cuttle.Workflow Workflow]]. It can be configured to `offset` the dependency.
  *
  * @param offset Time offest to apply when computing the partitions dependencies.
  */
case class TimeSeriesDependency(offset: Duration)

/** Configure a [[com.criteo.cuttle.Job Job]] as a [[TimeSeries]] job,
  *
  * @param calendar The calendar partitions configuration for this job (for example hourly or daily).
  * @param start The start instant at which this job must start being executed.
  * @param maxPeriods The maximum number of partitions the job can handle at once. If this is defined
  *                   to a value more than `1` and if possible, the scheduler can trigger [[com.criteo.cuttle.Execution Executions]]
  *                   for more than 1 partition at once.
  */
case class TimeSeries(calendar: TimeSeriesCalendar, start: Instant, end: Option[Instant] = None, maxPeriods: Int = 1)
    extends Scheduling {
  type Context = TimeSeriesContext
  type DependencyDescriptor = TimeSeriesDependency
  def toJson: Json =
    Json.obj(
      "start" -> start.asJson,
      "end" -> end.asJson,
      "maxPeriods" -> maxPeriods.asJson,
      "calendar" -> calendar.asJson
    )
}

/** [[TimeSeries]] utilities. */
object TimeSeries {
  /* Provide a default [[TimeSeriesScheduler]] for [[TimeSeries]] scheduling. */
  implicit def scheduler(implicit logger: Logger) = TimeSeriesScheduler(logger)
}

private[timeseries] sealed trait JobState
private[timeseries] object JobState {
  case object Done extends JobState
  case class Todo(maybeBackfill: Option[Backfill]) extends JobState
  case class Running(executionId: String) extends JobState

  import TimeSeriesUtils._
  implicit val encoder: Encoder[JobState] = deriveEncoder
  implicit def decoder(implicit jobs: Set[TimeSeriesJob]): Decoder[JobState] =
    deriveDecoder
  implicit val eqInstance: Eq[JobState] = Eq.fromUniversalEquals[JobState]
}

/** A [[TimeSeriesScheduler]] executes the [[com.criteo.cuttle.Workflow Workflow]] for the
  * time partitions defined in a calendar. Each [[com.criteo.cuttle.Job Job]] defines how it mnaps
  * to the calendar (for example Hourly or Daily UTC), and the [[com.criteo.cuttle.Scheduler Scheduler]]
  * ensure that at least one [[com.criteo.cuttle.Execution Execution]] is created and successfully run
  * for each defined Job/Period.
  *
  * The scheduler also allow to [[Backfill]] already computed partitions. The [[Backfill]] can be recursive
  * or not and an audit log of backfills is kept.
  */
case class TimeSeriesScheduler(logger: Logger) extends Scheduler[TimeSeries] with TimeSeriesApp {
  import JobState.{Done, Running, Todo}
  import TimeSeriesUtils._

  val allContexts = Database.sqlGetContextsBetween(None, None)

  private val _state = Ref(Map.empty[TimeSeriesJob, IntervalMap[Instant, JobState]])

  private val _backfills = Ref(Set.empty[Backfill])

  private[timeseries] def state: (State, Set[Backfill]) = atomic { implicit txn =>
    (_state(), _backfills())
  }

  private[timeseries] def backfillJob(name: String,
                                      description: String,
                                      jobs: Set[TimeSeriesJob],
                                      start: Instant,
                                      end: Instant,
                                      priority: Int,
                                      xa: XA)(implicit user: Auth.User): IO[Boolean] = {
    val (isValid, newBackfill) = atomic { implicit txn =>
      val id = UUID.randomUUID().toString
      val newBackfill = Backfill(id, start, end, jobs, priority, name, description, "RUNNING", user.userId)

      val valid = for {
        job <- jobs
      } yield {
        val st = _state().apply(job).intersect(Interval(start, end))
        val calendar = job.scheduling.calendar
        val validIn = st.toList
          .collect {
            case (Interval(Finite(lo), Finite(hi)), Done)
                if (calendar.truncate(lo) == lo && calendar.truncate(hi) == hi) =>
              (lo, hi)
          }
          .sortBy(_._1)
        validIn.nonEmpty &&
        validIn.head._1 == start &&
        validIn.last._2 == end &&
        validIn.zip(validIn.tail).forall { case (pred, next) => pred._2 == next._1 }
      }

      val isValid = valid.forall(x => x)
      if (isValid) {
        _backfills() = _backfills() + newBackfill
        _state() = _state() ++ jobs.map((job: TimeSeriesJob) =>
          job -> (_state().apply(job).update(Interval(start, end), Todo(Some(newBackfill)))))
      }
      (isValid, newBackfill)
    }
    if (isValid)
      Database.createBackfill(newBackfill).transact(xa).map(_ => true)
    else
      IO.pure(false)
  }

  def start(workflow: Workflow[TimeSeries], executor: Executor[TimeSeries], xa: XA, logger: Logger): Unit = {
    logger.info("Validate workflow before start")
    TimeSeriesUtils.validate(workflow) match {
      case Left(errors) =>
        val consolidatedError = errors.mkString("\n")
        logger.error(consolidatedError)
        throw new IllegalArgumentException(consolidatedError)
      case Right(_) => ()
    }
    logger.info("Workflow is valid")

    Database.doSchemaUpdates.transact(xa).unsafeRunSync

    Database
      .deserializeState(workflow.vertices)
      .transact(xa)
      .unsafeRunSync
      .foreach { state =>
        atomic { implicit txn =>
          _state() = state
        }
      }

    atomic { implicit txn =>
      val incompleteBackfills = Database
        .queryBackfills(Some(sql"""status = 'RUNNING'"""))
        .list
        .map(_.map {
          case (id, name, description, jobsIdsString, priority, start, end, _, status, createdBy) =>
            val jobsIds = jobsIdsString.split(",")
            val jobs = workflow.vertices.filter { job =>
              jobsIds.contains(job.id)
            }
            Backfill(id, start, end, jobs, priority, name, description, status, createdBy)
        })
        .transact(xa)
        .unsafeRunSync

      _backfills() = _backfills() ++ incompleteBackfills

      workflow.vertices.foreach { job =>
        val definedInterval =
          Interval(Finite(job.scheduling.start), job.scheduling.end.map(Finite.apply _).getOrElse(Top))
        val oldJobState = _state().getOrElse(job, IntervalMap.empty[Instant, JobState])
        val missingIntervals = IntervalMap(definedInterval -> (()))
          .whenIsUndef(oldJobState.intersect(definedInterval))
          .toList
          .map(_._1)
        val jobState = missingIntervals.foldLeft(oldJobState) { (st, interval) =>
          st.update(interval, Todo(None))
        }
        _state() = _state() + (job -> jobState)
      }
    }

    def mainLoop(running: Set[Run]): Unit = {
      val (completed, stillRunning) = running.partition {
        case (_, _, effect) => effect.isCompleted
      }

      val (stateSnapshot, completedBackfills, toRun) = atomic { implicit txn =>
        val (stateSnapshot, newBackfills, completedBackfills) =
          collectCompletedJobs(_state(), _backfills(), completed)

        val toRun = jobsToRun(workflow, stateSnapshot, Instant.now)

        _state() = stateSnapshot
        _backfills() = newBackfills

        (stateSnapshot, completedBackfills, toRun)
      }

      val newExecutions = executor.runAll(toRun)

      atomic { implicit txn =>
        _state() = newExecutions.foldLeft(_state()) {
          case (st, (execution, _)) =>
            st + (execution.job ->
              st(execution.job).update(execution.context.toInterval, Running(execution.id)))
        }
      }

      if (completed.nonEmpty || toRun.nonEmpty)
        runOrLogAndDie(Database.serializeState(stateSnapshot).transact(xa).unsafeRunSync,
                       "TimeseriesScheduler, cannot serialize state, shutting down")

      if (completedBackfills.nonEmpty)
        runOrLogAndDie(
          Database
            .setBackfillStatus(completedBackfills.map(_.id), "COMPLETE")
            .transact(xa)
            .unsafeRunSync,
          "TimeseriesScheduler, cannot serialize state, shutting down"
        )

      val newRunning = stillRunning ++ newExecutions.map {
        case (execution, result) =>
          (execution.job, execution.context, result)
      }

      utils.Timeout(ScalaDuration.create(1, "s")).andThen {
        case _ => mainLoop(newRunning)
      }
    }

    mainLoop(Set.empty)
  }

  private def runOrLogAndDie(thunk: => Unit, message: => String): Unit = {
    import java.io._

    try {
      thunk
    } catch {
      case (e: Throwable) => {
        logger.error(message)
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        logger.error(sw.toString)
        System.exit(-1)
      }
    }
  }

  private[timeseries] def collectCompletedJobs(state: State,
                                               backfills: Set[Backfill],
                                               completed: Set[Run]): (State, Set[Backfill], Set[Backfill]) = {

    // update state with job statuses
    val newState = completed.foldLeft(state) {
      case (acc, (job, context, future)) =>
        val jobState = if (future.value.get.isSuccess) Done else Todo(context.backfill)
        acc + (job -> (acc(job).update(context.toInterval, jobState)))
    }

    val notCompletedBackfills = backfills.filter { bf =>
      val itvl = Interval(bf.start, bf.end)
      bf.jobs.exists(job => newState(job).intersect(itvl).toList.exists(_._2 != Done))
    }

    (newState, notCompletedBackfills, backfills -- notCompletedBackfills)
  }

  private[timeseries] def jobsToRun(workflow: Workflow[TimeSeries], state0: State, now: Instant): List[Executable] = {

    val timerInterval = Interval(Bottom, Finite(now))
    val state = state0.mapValues(_.intersect(timerInterval))

    val parentsMap = workflow.edges.groupBy { case (child, _, _)   => child }
    val childrenMap = workflow.edges.groupBy { case (_, parent, _) => parent }

    workflow.vertices.toList.flatMap { job =>
      val full = IntervalMap[Instant, Unit](Interval[Instant](Bottom, Top) -> (()))
      val dependenciesSatisfied = parentsMap
        .getOrElse(job, Set.empty)
        .map {
          case (_, parent, lbl) =>
            state(parent).mapKeys(_.plus(lbl.offset)).collect { case Done => () }
        }
        .fold(full)(_ whenIsDef _)
      val noChildrenRunning = childrenMap
        .getOrElse(job, Set.empty)
        .map {
          case (child, _, lbl) =>
            state(child).mapKeys(_.minus(lbl.offset)).collect { case Running(_) => () }
        }
        .fold(full)(_ whenIsUndef _)
      val toRun = state(job)
        .collect { case Todo(maybeBackfill) => maybeBackfill }
        .whenIsDef(dependenciesSatisfied)
        .whenIsDef(noChildrenRunning)

      for {
        (interval, maybeBackfill) <- toRun.toList
        (lo, hi) <- job.scheduling.calendar.inInterval(interval, job.scheduling.maxPeriods)
      } yield {
        (job, TimeSeriesContext(lo, hi, maybeBackfill))
      }
    }
  }

  private def getRunningBackfillsSize(jobs: Set[String]) = {
    val runningBackfills = state match {
      case (_, backfills) =>
        backfills.filter(
          bf =>
            bf.status == "RUNNING" &&
              bf.jobs.map(_.id).intersect(jobs).nonEmpty)
    }

    runningBackfills.size
  }

  /**
    * We compute the last instant when job was in a valid state
    * @param jobs set of jobs to process
    * @return Iterable of job to to last instant when job was in a valid state.
    *         Iterable is empty when job doesn't contain any "DONE" interval.
    */
  private def getTimeOfLastSuccess(jobs: Set[String]) =
    _state
      .single()
      .collect {
        case (job, intervals) if jobs.contains(job.id) =>
          val intervalList = intervals.toList
          val lastValidInterval = intervalList.takeWhile {
            case (_, Running(_)) => false
            case (_, Todo(None)) => false
            case _               => true
          }.lastOption

          lastValidInterval.map {
            case (interval, _) =>
              job -> (interval.hi match {
                case Finite(instant) => instant
                case _               => Instant.MAX
              })
          }
      }
      .flatten
      .toSeq

  override def getMetrics(jobs: Set[String], workflow: Workflow[TimeSeries]): Seq[Metric] = {
    val lastSuccessTime = getTimeOfLastSuccess(jobs)
    val secondsSinceLastSuccess = lastSuccessTime.foldLeft(
      Gauge(
        "cuttle_timeseries_scheduler_last_success_epoch_seconds",
        "The seconds since a job's last success with all previous executions being successful"
      )
    ) {
      case (gauge, (job, lastSuccess)) =>
        gauge.labeled(
          Set("job_id" -> job.id, "job_name" -> job.name),
          Instant.now.getEpochSecond - lastSuccess.getEpochSecond
        )
    }

    val absoluteLatency = lastSuccessTime.map {
      case (job, lastSuccess) => job -> getAbsoluteLatency(job, lastSuccess)
    }.toMap

    val latencies = List(
      Gauge(
        "cuttle_timeseries_scheduler_absolute_latency_epoch_seconds",
        "Absolute latency of a job in seconds, with respect to its last success with all previous executions being successful"
      ) -> absoluteLatency,
      Gauge(
        "cuttle_timeseries_scheduler_relative_latency_epoch_seconds",
        "Relative latency of a job in seconds, taking into account its parents' latencies"
      ) -> absoluteLatency.map {
        case (job, latency) =>
          val maxParentLatency = workflow.edges
            .collect { case (v, dep, _) if v == job => dep }
            .flatMap(absoluteLatency.get)
            .foldLeft(0L)(Math.max)
          job -> Math.max(0, latency - maxParentLatency)
      }
    ).map {
      case (gauge, latencyValues) =>
        latencyValues.foldLeft(gauge) {
          case (gauge, (job, latency)) =>
            val tags = if (!job.tags.isEmpty) Set("tags" -> job.tags.map(_.name).mkString(",")) else Nil
            gauge.labeled(Set("job_id" -> job.id, "job_name" -> job.name) ++ tags, latency)
        }
    }

    Seq(
      Gauge("cuttle_timeseries_scheduler_stat_count", "The number of backfills")
        .labeled("type" -> "backfills", getRunningBackfillsSize(jobs)),
      secondsSinceLastSuccess
    ) ++ latencies
  }

  override def getStats(jobs: Set[String]): Json =
    Map("backfills" -> getRunningBackfillsSize(jobs)).asJson

  private def getAbsoluteLatency(job: TimeSeriesJob, lastSuccess: Instant): Long = {
    val expectedSuccess = job.scheduling.calendar match {
      case Hourly        => Instant.now.minus(1, HOURS)
      case Daily(tz)     => Instant.now.atZone(tz).minus(1, DAYS).toInstant
      case Weekly(tz, _) => Instant.now.atZone(tz).minus(1, WEEKS).toInstant
      case Monthly(tz)   => Instant.now.atZone(tz).minus(1, MONTHS).toInstant
    }
    if (expectedSuccess.compareTo(lastSuccess) <= 0) 0L else expectedSuccess.getEpochSecond - lastSuccess.getEpochSecond
  }
}

private[timeseries] object TimeSeriesUtils {
  type TimeSeriesJob = Job[TimeSeries]
  type Executable = (TimeSeriesJob, TimeSeriesContext)
  type Run = (TimeSeriesJob, TimeSeriesContext, Future[Completed])
  type State = Map[TimeSeriesJob, IntervalMap[Instant, JobState]]

  val UTC: ZoneId = ZoneId.of("UTC")

  /**
    * Validation of cycle absence in workflow DAG and an absence the (execution, dependency) tuple that execution has
    * a start date after an execution's start date.
    * It's implemented based on Kahn's algorithm.
    * @param workflow workflow to be validated
    * @return either a validation errors list or an unit
    */
  def validate(workflow: Workflow[TimeSeries]): Either[List[String], Unit] = {
    val errors = collection.mutable.ListBuffer.empty[String]
    val edges = collection.mutable.Set(workflow.edges.toSeq: _*)
    val roots = collection.mutable.Set(workflow.roots.toSeq: _*)

    while (roots.nonEmpty) {
      val root = roots.head

      roots.remove(root)

      val edgesWithoutParent = edges.filter(_._2 == root)

      edgesWithoutParent.foreach {
        case edge @ (child, _, _) =>
          if (child.scheduling.start.isBefore(root.scheduling.start)) {
            errors += s"Job [${child.id}] starts at [${child.scheduling.start.toString}] " +
              s"before his parent [${root.id}] at [${root.scheduling.start.toString}]"
          }

          edges.remove(edge)

          if (!edges.exists(_._1 == child)) {
            roots.add(child)
          }
      }
    }

    if (edges.nonEmpty) errors += "Workflow has at least one cycle"

    if (errors.nonEmpty) Left(errors.toList)
    else Right(())
  }
}
