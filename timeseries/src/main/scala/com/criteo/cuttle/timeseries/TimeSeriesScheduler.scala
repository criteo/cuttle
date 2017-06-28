package com.criteo.cuttle.timeseries

import Internal._
import com.criteo.cuttle._

import scala.concurrent._
import scala.concurrent.duration.{Duration => ScalaDuration}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.stm._

import cats.implicits._

import algebra.lattice.Bool._

import io.circe._
import io.circe.syntax._
import io.circe.generic.semiauto._

import doobie.imports._

import java.time._
import java.time.temporal.ChronoUnit._
import java.time.temporal._
import java.util.UUID

import continuum.{Interval, IntervalSet}
import continuum.bound._

sealed trait TimeSeriesGrid
object TimeSeriesGrid {
  case object Hourly extends TimeSeriesGrid
  case class Daily(tz: ZoneId) extends TimeSeriesGrid
  private[timeseries] case object Continuous extends TimeSeriesGrid

  implicit val gridEncoder = new Encoder[TimeSeriesGrid] {
    override def apply(grid: TimeSeriesGrid) = grid match {
      case Hourly => Json.obj("period" -> "hourly".asJson)
      case Daily(tz: ZoneId) =>
        Json.obj(
          "period" -> "daily".asJson,
          "zoneId" -> tz.getId().asJson
        )
      case Continuous => Json.obj("period" -> "continuuous".asJson)
    }
  }
}

import TimeSeriesGrid._

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

  def toInterval: Interval[Instant] = Interval.closedOpen(start, end)

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

case class TimeSeriesScheduler() extends Scheduler[TimeSeries] with TimeSeriesApp {
  import TimeSeriesUtils._

  val allContexts = Database.sqlGetContextsBetween(None, None)

  private val timer =
    Job("timer", TimeSeries(Continuous, Instant.ofEpochMilli(0)))(_ => sys.error("panic!"))

  private val _state = Ref(Map.empty[TimeSeriesJob, IntervalSet[Instant]])
  private val _backfills = TSet.empty[Backfill]
  private val _running = Ref(Map.empty[TimeSeriesJob, IntervalSet[Instant]])

  private[timeseries] def state: (State, State, Set[Backfill]) = atomic { implicit txn =>
    (_state(), _running(), _backfills.snapshot)
  }

  private[timeseries] def backfillJob(name: String,
                                      description: String,
                                      jobs: Set[TimeSeriesJob],
                                      start: Instant,
                                      end: Instant,
                                      priority: Int,
                                      xa: XA) = {
    val (result, newBackfill) = atomic { implicit txn =>
      val id = UUID.randomUUID().toString
      val newBackfill = Backfill(id, start, end, jobs, priority, name, description, "RUNNING")
      val newBackfillDomain = backfillDomain(newBackfill)
      val result = if (jobs.exists(job => newBackfill.start.isBefore(job.scheduling.start))) {
        Left("Cannot backfill before a job's start date")
      } else if (_backfills.exists(backfill => and(backfillDomain(backfill), newBackfillDomain) != zero[StateD])) {
        Left("Intersects with another backfill")
      } else if (newBackfillDomain.defined.exists {
                   case (job, is) =>
                     is.exists { interval =>
                       IntervalSet(interval) -- IntervalSet(
                         splitInterval(job, interval, true).map(_.toInterval).toSeq: _*) != IntervalSet.empty[Instant]
                     }
                 }) {
        Left("Cannot backfill partial periods")
      } else {
        _backfills += newBackfill
        _state() = _state() ++ jobs.map((job: TimeSeriesJob) =>
          job -> (_state().apply(job) - Interval.closedOpen(start, end)))
        Right(id)
      }
      (result, newBackfill)
    }
    if (result.isRight)
      Database.createBackfill(newBackfill).transact(xa).unsafePerformIO
    result
  }

  private def backfillDomain(backfill: Backfill) =
    StateD(backfill.jobs.map(job => job -> IntervalSet(Interval.closedOpen(backfill.start, backfill.end))).toMap)

  def start(workflow: Workflow[TimeSeries], executor: Executor[TimeSeries], xa: XA): Unit = {
    Database.doSchemaUpdates.transact(xa).unsafePerformIO

    Database
      .deserializeState(workflow.vertices)
      .transact(xa)
      .unsafePerformIO
      .foreach {
        case (state) =>
          atomic { implicit txn =>
            _state() = _state() ++ state
          }
      }

    //TODO Load uncomplete backfills > _backfills ++= backfillState

    atomic { implicit txn =>
      workflow.vertices.foreach { job =>
        if (!_state().contains(job)) {
          _state() = _state() + (job -> IntervalSet.empty[Instant])
        }
        _running() = _running() + (job -> IntervalSet.empty[Instant])
      }
    }

    def addRuns(runs: Set[Run])(implicit txn: InTxn) =
      runs.foreach {
        case (job, context, future) =>
          _running() = _running() + (job -> (_running().apply(job) - context.toInterval))
          if (future.value.get.isSuccess) {
            _state() = _state() + (job -> (_state().apply(job) + context.toInterval))
          }
      }

    def go(running: Set[Run]): Unit = {
      val (completed, stillRunning) = running.partition(_._3.isCompleted)
      val now = Instant.now
      val (stateSnapshot, completedBackfills, toRun) = atomic { implicit txn =>
        addRuns(completed)
        val completedBackfills =
          _backfills.filter(bf => without(backfillDomain(bf), StateD(_state())) =!= zero[StateD])
        _backfills --= completedBackfills
        val _toRun = next(
          workflow,
          _state(),
          _backfills.snapshot,
          stillRunning.map { case (job, context, _) => (job, context) },
          IntervalSet(Interval.lessThan(now))
        )
        val newRunning = (for {
          (job, context) <- _toRun
        } yield StateD(Map(job -> IntervalSet(context.toInterval))))
          .fold(zero[StateD])(or)

        _running() = or(StateD(_running()), newRunning).defined
        (_state(), completedBackfills.snapshot, _toRun)
      }

      if (completed.nonEmpty || toRun.nonEmpty)
        Database.serializeState(stateSnapshot).transact(xa).unsafePerformIO

      if (completedBackfills.nonEmpty)
        Database.setBackfillStatus(completedBackfills.map(_.id), "COMPLETE").transact(xa).unsafePerformIO

      val newRunning = stillRunning ++ executor.runAll(toRun).map {
        case (execution, result) =>
          (execution.job, execution.context, result)
      }

      Future.firstCompletedOf(utils.Timeout(ScalaDuration.create(1, "s")) :: newRunning.map(_._3).toList).andThen {
        case _ => go(newRunning)
      }
    }

    go(Set.empty)
  }

  private[timeseries] def split(start: Instant,
                                end: Instant,
                                tz: ZoneId,
                                unit: ChronoUnit,
                                conservative: Boolean,
                                maxPeriods: Int): Iterator[TimeSeriesContext] = {
    val List(zonedStart, zonedEnd) = List(start, end).map { t =>
      t.atZone(UTC).withZoneSameInstant(tz)
    }

    def findBound(t: ZonedDateTime, before: Boolean) = {
      val truncated = t.truncatedTo(unit)
      if (before)
        truncated
      else if (truncated == t)
        t
      else
        truncated.plus(1, unit)
    }

    val alignedStart = findBound(zonedStart, !conservative)
    val alignedEnd = findBound(zonedEnd, conservative)

    val periods = alignedStart.until(alignedEnd, unit)

    (0L to (periods - 1)).grouped(maxPeriods).map { l =>
      def alignedNth(k: Long) =
        alignedStart
          .plus(k, unit)
          .withZoneSameInstant(UTC)
          .toInstant

      TimeSeriesContext(alignedNth(l.head), alignedNth(l.last + 1))
    }
  }

  private[timeseries] def splitInterval(job: TimeSeriesJob, interval: Interval[Instant], mode: Boolean = true) = {
    val (unit, tz) = job.scheduling.grid match {
      case Hourly => (HOURS, UTC)
      case Daily(_tz) => (DAYS, _tz)
      case Continuous => sys.error("panic!")
    }
    val Closed(start) = interval.lower.bound
    val Open(end) = interval.upper.bound
    val maxPeriods = if (mode) job.scheduling.maxPeriods else 1
    split(start, end, tz, unit, mode, maxPeriods)
  }

  private[timeseries] def next(workflow0: Workflow[TimeSeries],
                               state0: State,
                               backfills: Set[Backfill],
                               running: Set[(TimeSeriesJob, TimeSeriesContext)],
                               timerInterval: IntervalSet[Instant]): List[Executable] = {
    val workflow = workflow0 dependsOn timer
    val state = state0 + (timer -> timerInterval)

    val runningIntervals = StateD {
      running
        .groupBy(_._1)
        .mapValues(_.map(x => x._2.toInterval)
          .foldLeft(IntervalSet.empty[Instant])((is, interval) => is + interval))
        .toMap
    }

    val dependencies =
      (for {
        (child, parent, TimeSeriesDependency(offset)) <- workflow.edges.toList
        is = state(parent)
      } yield
        StateD(Map(child -> is.map(itvl => itvl.map(_.plus(offset)))), IntervalSet(Interval.full))).reduce(and(_, _))

    val runningDependencies = // only needed when backfilling
      (for {
        (child, parent, TimeSeriesDependency(offset)) <- workflow.edges.toList
        is = runningIntervals.get(child)
      } yield StateD(Map(parent -> is.map(itvl => itvl.map(_.minus(offset)))), IntervalSet.empty)).reduce(or(_, _))

    val jobDomain = StateD(
      workflow.vertices.map(job => job -> IntervalSet(Interval.atLeast(job.scheduling.start))).toMap)

    val ready = Seq(complement(runningIntervals),
                    complement(StateD(state)),
                    jobDomain,
                    dependencies,
                    complement(runningDependencies))
      .reduce(and(_, _))

    val toBackfill: Map[Backfill, StateD] =
      backfills.map { backfill =>
        backfill -> and(ready, backfillDomain(backfill))
      }.toMap

    val toRunNormally = without(ready, toBackfill.values.fold(zero[StateD])(or(_, _)))

    val toRun: Map[Option[Backfill], State] =
      toBackfill.map {
        case (backfill, StateD(st, _)) =>
          (Some(backfill): Option[Backfill]) -> st
      } + (None -> toRunNormally.defined)

    for {
      (maybeBackfill, state) <- toRun.toList
      (job, intervalSet) <- state.toList.filterNot { case (job, _) => job == timer }
      interval <- intervalSet
      context <- splitInterval(job, interval)
    } yield (job, context.copy(backfill = maybeBackfill))
  }
}

private[timeseries] object TimeSeriesUtils {
  type TimeSeriesJob = Job[TimeSeries]
  type State = Map[TimeSeriesJob, IntervalSet[Instant]]
  type Executable = (TimeSeriesJob, TimeSeriesContext)
  type Run = (TimeSeriesJob, TimeSeriesContext, Future[Unit])

  val UTC: ZoneId = ZoneId.of("UTC")

  val emptyIntervalSet: IntervalSet[Instant] = IntervalSet.empty[Instant]
}
