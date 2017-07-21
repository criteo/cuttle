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

  implicit val gridEncoder = new Encoder[TimeSeriesGrid] {
    override def apply(grid: TimeSeriesGrid) = grid match {
      case Hourly => Json.obj("period" -> "hourly".asJson)
      case Daily(tz: ZoneId) =>
        Json.obj(
          "period" -> "daily".asJson,
          "zoneId" -> tz.getId().asJson
        )
    }
  }
}

case class Backfill(id: String,
                    start: Instant,
                    end: Instant,
                    jobs: Set[Job[TimeSeries]],
                    priority: Int,
                    name: String,
                    description: String,
                    status: String)

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
  implicit def scheduler = TimeSeriesScheduler()
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

case class TimeSeriesScheduler() extends Scheduler[TimeSeries] with TimeSeriesApp {
  import TimeSeriesUtils._
  import JobState.{Done, Running, Todo}

  val allContexts = Database.sqlGetContextsBetween(None, None)

  private val _state = Ref(Map.empty[TimeSeriesJob, IntervalMap[Instant, JobState]])

  private val _backfills = TSet.empty[Backfill]

  private[timeseries] def state: (State, Set[Backfill]) = atomic { implicit txn =>
    (_state(), _backfills.snapshot)
  }

  private[timeseries] def backfillJob(name: String,
                                      description: String,
                                      jobs: Set[TimeSeriesJob],
                                      start: Instant,
                                      end: Instant,
                                      priority: Int,
                                      xa: XA) = {
    val (isValid, newBackfill) = atomic { implicit txn =>
      val id = UUID.randomUUID().toString
      val newBackfill = Backfill(id, start, end, jobs, priority, name, description, "RUNNING")

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
        _backfills += newBackfill
        _state() = _state() ++ jobs.map((job: TimeSeriesJob) =>
          job -> (_state().apply(job).update(Interval(start, end), Todo(Some(newBackfill)))))
      }
      (isValid, newBackfill)
    }
    if (isValid)
      Database.createBackfill(newBackfill).transact(xa).unsafePerformIO
    isValid
  }

  def start(workflow: Workflow[TimeSeries], executor: Executor[TimeSeries], xa: XA): Unit = {
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
      val uncompletedBackfills = Database
        .queryBackfills(Some(sql"""status = 'RUNNING'"""))
        .list
        .map(_.map {
          case (id, name, description, jobsIdsString, priority, start, end, created_at, status) =>
            val jobsIds = jobsIdsString.split(",")
            val jobs = workflow.vertices.filter { job => jobsIds.contains(job.id) }
            Backfill(id, start, end, jobs, priority, name, description, status)
        })
        .transact(xa)
        .unsafePerformIO

      _backfills ++= uncompletedBackfills

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

    def go(running: Set[Run]): Unit = {
      val (completed, stillRunning) = running.partition(_._3.isCompleted)
      val now = Instant.now
      val (stateSnapshot, completedBackfills, toRun) = atomic { implicit txn =>
        completed.foreach {
          case (job, context, future) =>
            val jobState = if (future.value.get.isSuccess) Done else Todo(context.backfill)
            _state() = _state() + (job ->
              (_state().apply(job).update(context.toInterval, jobState)))
        }

        val oldBackfills = _backfills.snapshot
        _backfills.retain { bf =>
          val itvl = Interval(bf.start, bf.end)
          !bf.jobs.forall(job => _state().apply(job).intersect(itvl).toList.forall(_._2 == Done))
        }
        val _toRun = next(workflow, _state(), now)

        (_state(), oldBackfills -- _backfills.snapshot, _toRun)
      }

      val newExecutions = executor.runAll(toRun)

      atomic { implicit txn =>
        _state() = newExecutions.foldLeft(_state()) { (st, x) =>
          val (execution, _) = x
          st + (execution.job ->
            st(execution.job).update(execution.context.toInterval, Running(execution.id)))
        }
      }

      if (completed.nonEmpty || toRun.nonEmpty)
        Database.serializeState(stateSnapshot).transact(xa).unsafePerformIO

      if (completedBackfills.nonEmpty)
        Database.setBackfillStatus(completedBackfills.map(_.id), "COMPLETE").transact(xa).unsafePerformIO

      val newRunning = stillRunning ++ newExecutions.map {
        case (execution, result) =>
          (execution.job, execution.context, result)
      }

      Future.firstCompletedOf(utils.Timeout(ScalaDuration.create(1, "s")) :: newRunning.map(_._3).toList).andThen {
        case _ => go(newRunning ++ stillRunning)
      }
    }

    go(Set.empty)
  }

  private[timeseries] def next(workflow: Workflow[TimeSeries], state0: State, now: Instant): List[Executable] = {
    val timerInterval = Interval(Bottom, Finite(now))
    val state = state0.mapValues(_.intersect(timerInterval))

    val parentsMap = workflow.edges.groupBy { case (child, parent, _) => child }
    val childrenMap = workflow.edges.groupBy { case (child, parent, _) => parent }

    (for {
      job <- workflow.vertices.toList
    } yield {
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
    }).flatten

  }

  override def getStats(jobs: Set[String]) = {
    val runningBackfills = state match {
      case (_, backfills) =>
        backfills.filter(bf =>
          bf.status == "RUNNING" &&
            bf.jobs.map(_.id).intersect(jobs).nonEmpty
        )
    }
    Map("backfills" -> runningBackfills.size).asJson
  }
}

private[timeseries] object TimeSeriesUtils {
  type TimeSeriesJob = Job[TimeSeries]
  type Executable = (TimeSeriesJob, TimeSeriesContext)
  type Run = (TimeSeriesJob, TimeSeriesContext, Future[Unit])
  type State = Map[TimeSeriesJob, IntervalMap[Instant, JobState]]

  val UTC = ZoneId.of("UTC")
}
