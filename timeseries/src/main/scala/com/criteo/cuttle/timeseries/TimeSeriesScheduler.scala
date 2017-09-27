package com.criteo.cuttle.timeseries

import Internal._
import com.criteo.cuttle._

import scala.concurrent._
import scala.concurrent.duration.{Duration => ScalaDuration}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.stm._
import cats.implicits._
import io.circe._
import io.circe.syntax._
import io.circe.generic.semiauto._
import doobie.imports._
import java.util.UUID
import java.time._
import java.time.temporal.ChronoUnit._
import java.time.ZoneOffset.UTC
import java.time.temporal.{ChronoUnit, TemporalAdjusters}

import com.criteo.cuttle.timeseries.TimeSeriesGrid.{Daily, Hourly, Monthly}
import intervals.{Bound, Interval, IntervalMap}
import Bound.{Bottom, Finite, Top}

sealed trait TimeSeriesGrid {
  def next(t: Instant): Instant
  def truncate(t: Instant): Instant
  def ceil(t: Instant): Instant = {
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
    }
  }
}

sealed trait TimeSeriesGridView {
  def grid: TimeSeriesGrid
  def upper(): TimeSeriesGridView
  val aggregationFactor: Int
}

object TimeSeriesGrid {

  case object Hourly extends TimeSeriesGrid {
    def truncate(t: Instant) = t.truncatedTo(HOURS)
    def next(t: Instant) =
      t.truncatedTo(HOURS).plus(1, HOURS)
  }
  case class Daily(tz: ZoneId) extends TimeSeriesGrid {
    def truncate(t: Instant) = t.atZone(tz).truncatedTo(DAYS).toInstant
    def next(t: Instant) = t.atZone(tz).truncatedTo(DAYS).plus(1, DAYS).toInstant
  }
  case class Monthly(tz: ZoneId) extends TimeSeriesGrid {
    private val truncateToMonth = (t: ZonedDateTime) =>
      t.`with`(TemporalAdjusters.firstDayOfMonth()).truncatedTo(ChronoUnit.DAYS)
    def truncate(t: Instant) = truncateToMonth(t.atZone(tz)).toInstant
    def next(t: Instant) = truncateToMonth(t.atZone(tz)).plus(1, MONTHS).toInstant
  }

  implicit val gridEncoder = new Encoder[TimeSeriesGrid] {
    override def apply(grid: TimeSeriesGrid) = grid match {
      case Hourly => Json.obj("period" -> "hourly".asJson)
      case Daily(tz: ZoneId) =>
        Json.obj(
          "period" -> "daily".asJson,
          "zoneId" -> tz.getId().asJson
        )
      case Monthly(tz: ZoneId) =>
        Json.obj(
          "period" -> "monthly".asJson,
          "zoneId" -> tz.getId().asJson
        )
    }
  }
}

object TimeSeriesGridView {
  def apply(grid: TimeSeriesGrid) = grid match {
    case TimeSeriesGrid.Hourly => new HourlyView(1)
    case TimeSeriesGrid.Daily(tz) => new DailyView(tz, 1)
    case TimeSeriesGrid.Monthly(tz) => new MonthlyView(tz, 1)
  }
  sealed trait GenericView extends TimeSeriesGridView {
    def over: (Int, TimeSeriesGrid)
    def grid = over._2
    def truncate(t: Instant) = grid.truncate(t)
    def next(t: Instant) = (1 to over._1).foldLeft(grid.truncate(t))((acc, _) => grid.next(acc))
    def upper(): TimeSeriesGridView
  }
  case class HourlyView(aggregationFactor: Int) extends GenericView {
    def over = (1, Hourly)
    override def upper: TimeSeriesGridView = new DailyView(UTC, aggregationFactor * 24)
  }
  case class DailyView(tz: ZoneId, aggregationFactor: Int) extends GenericView {
    def over = (1, Daily(tz))
    override def upper: TimeSeriesGridView = new WeeklyView(tz, aggregationFactor * 7)
  }
  case class WeeklyView(tz: ZoneId, aggregationFactor: Int) extends GenericView {
    def over = (7, Daily(tz))
    override def upper: TimeSeriesGridView = new MonthlyView(tz, aggregationFactor * 4)
  }
  case class MonthlyView(tz: ZoneId, aggregationFactor: Int) extends GenericView {
    def over = (1, Monthly(tz))
    override def upper: TimeSeriesGridView = new MonthlyView(tz, 1)
  }
}

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
  implicit val encoder: Encoder[Backfill] = deriveEncoder
  implicit def decoder(implicit jobs: Set[Job[TimeSeries]]) =
    deriveDecoder[Backfill]
}

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

object TimeSeriesContext {
  private[timeseries] implicit val encoder: Encoder[TimeSeriesContext] = deriveEncoder
  private[timeseries] implicit def decoder(implicit jobs: Set[Job[TimeSeries]]): Decoder[TimeSeriesContext] =
    deriveDecoder
}

case class TimeSeriesDependency(offset: Duration)

case class TimeSeries(grid: TimeSeriesGrid, start: Instant, maxPeriods: Int = 1) extends Scheduling {
  import TimeSeriesGrid._
  type Context = TimeSeriesContext
  type DependencyDescriptor = TimeSeriesDependency
  def toJson: Json =
    Json.obj(
      "start" -> start.asJson,
      "maxPeriods" -> maxPeriods.asJson,
      "grid" -> grid.asJson
    )
}

object TimeSeries {
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
}

case class TimeSeriesScheduler(logger: Logger) extends Scheduler[TimeSeries] with TimeSeriesApp {
  import TimeSeriesUtils._
  import JobState.{Done, Running, Todo}

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
                                      xa: XA)(implicit user: User) = {
    val (isValid, newBackfill) = atomic { implicit txn =>
      val id = UUID.randomUUID().toString
      val newBackfill = Backfill(id, start, end, jobs, priority, name, description, "RUNNING", user.userId)

      val valid = for {
        job <- jobs
      } yield {
        val st = _state().apply(job).intersect(Interval(start, end))
        val grid = job.scheduling.grid
        val validIn = st.toList
          .collect {
            case (Interval(Finite(lo), Finite(hi)), Done) if (grid.truncate(lo) == lo && grid.truncate(hi) == hi) =>
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
      Database.createBackfill(newBackfill).transact(xa).unsafePerformIO
    isValid
  }

  def start(workflow: Workflow[TimeSeries], executor: Executor[TimeSeries], xa: XA, logger: Logger): Unit = {
    Database.doSchemaUpdates.transact(xa).unsafePerformIO

    Database
      .deserializeState(workflow.vertices)
      .transact(xa)
      .unsafePerformIO
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
          case (id, name, description, jobsIdsString, priority, start, end, created_at, status, created_by) =>
            val jobsIds = jobsIdsString.split(",")
            val jobs = workflow.vertices.filter { job =>
              jobsIds.contains(job.id)
            }
            Backfill(id, start, end, jobs, priority, name, description, status, created_by)
        })
        .transact(xa)
        .unsafePerformIO

      _backfills() = _backfills() ++ incompleteBackfills

      workflow.vertices.foreach { job =>
        val definedInterval = Interval(Finite(job.scheduling.start), Top)
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
        case(_,_,effect) => effect.isCompleted
      }

      val now = Instant.now
      val (stateSnapshot, completedBackfills, toRun) = atomic { implicit txn =>
        val (stateSnapshot, newBackfills, completedBackfills) =
          collectCompletedJobs(workflow, now, _state(), _backfills(), completed)

        val toRun = jobsToRun(workflow, stateSnapshot, now)

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
        runOrLogAndDie(
          Database.serializeState(stateSnapshot).transact(xa).unsafePerformIO,
          "TimeseriesScheduler, cannot serialize state, shutting down")

      if (completedBackfills.nonEmpty)
        runOrLogAndDie(
	        Database
            .setBackfillStatus(completedBackfills.map(_.id), "COMPLETE")
            .transact(xa)
            .unsafePerformIO,
          "TimeseriesScheduler, cannot serialize state, shutting down")

      val newRunning = stillRunning ++ newExecutions.map {
        case (execution, result) =>
          (execution.job, execution.context, result)
      }

      Future.firstCompletedOf(
        utils.Timeout(ScalaDuration.create(1, "s")) :: newRunning.map(_._3).toList)
          .andThen {
            case _ => mainLoop(newRunning ++ stillRunning)
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

  private[timeseries] def collectCompletedJobs(
    workflow : Workflow[TimeSeries],
    now : Instant, 
    state : State,
    backfills : Set[Backfill],
    completed : Set[Run]) : (State, Set[Backfill], Set[Backfill]) = {

      // update state with job statuses
      val newState = completed.foldLeft(state) {
        case (acc, (job, context, future)) =>
          val jobState = if (future.value.get.isSuccess) Done else Todo(context.backfill)
          acc + (job ->(acc(job).update(context.toInterval, jobState)))
      }

      val notCompletedBackfills = backfills.filter { bf =>
        val itvl = Interval(bf.start, bf.end)
        bf.jobs.exists(job => newState(job).intersect(itvl).toList.exists(_._2 != Done))
      }

      (newState, notCompletedBackfills, backfills -- notCompletedBackfills)
  }

  private[timeseries] def jobsToRun(
    workflow: Workflow[TimeSeries], 
    state0: State, 
    now: Instant): List[Executable] = {

    val timerInterval = Interval(Bottom, Finite(now))
    val state = state0.mapValues(_.intersect(timerInterval))

    val parentsMap = workflow.edges.groupBy { case (child, _, _) => child }
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
        (lo, hi) <- job.scheduling.grid.inInterval(interval, job.scheduling.maxPeriods)
      } yield {
        (job, TimeSeriesContext(lo, hi, maybeBackfill))
      }
    }
  }

  override def getStats(jobs: Set[String]): Map[String, Long] = {
    val runningBackfills = state match {
      case (_, backfills) =>
        backfills.filter(
          bf =>
            bf.status == "RUNNING" &&
              bf.jobs.map(_.id).intersect(jobs).nonEmpty)
    }
    Map("backfills" -> runningBackfills.size)
  }
}

private[timeseries] object TimeSeriesUtils {
  type TimeSeriesJob = Job[TimeSeries]
  type Executable = (TimeSeriesJob, TimeSeriesContext)
  type Run = (TimeSeriesJob, TimeSeriesContext, Future[Completed])
  type State = Map[TimeSeriesJob, IntervalMap[Instant, JobState]]

  val UTC = ZoneId.of("UTC")
}