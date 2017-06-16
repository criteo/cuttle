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

import continuum.{Interval, IntervalSet}
import continuum.bound._

sealed trait TimeSeriesGrid
object TimeSeriesGrid {
  case object Hourly extends TimeSeriesGrid
  case class Daily(tz: ZoneId) extends TimeSeriesGrid
  private[timeseries] case object Continuous extends TimeSeriesGrid
}

import TimeSeriesGrid._

case class Backfill(id: String, start: Instant, end: Instant, jobs: Set[Job[TimeSeriesScheduling]], priority: Int)
private[timeseries] object Backfill {
  implicit val encoder: Encoder[Backfill] = deriveEncoder
  implicit def decoder(implicit jobs: Set[Job[TimeSeriesScheduling]]) =
    deriveDecoder[Backfill]
}

case class TimeSeriesContext(start: Instant, end: Instant, backfill: Option[Backfill] = None)
    extends SchedulingContext {
  import TimeSeriesUtils._

  def toJson: Json = this.asJson

  def log: ConnectionIO[String] = Database.serializeContext(this)

  def toInterval: Interval[Instant] = Interval.closedOpen(start, end)
}

object TimeSeriesContext {
  import TimeSeriesUtils._

  private[timeseries] implicit val encoder: Encoder[TimeSeriesContext] = deriveEncoder
  private[timeseries] implicit def decoder(implicit jobs: Set[Job[TimeSeriesScheduling]]): Decoder[TimeSeriesContext] =
    deriveDecoder

  implicit val ordering: Ordering[TimeSeriesContext] = {
    implicit val maybeBackfillOrdering: Ordering[Option[Backfill]] = {
      Ordering.by(maybeBackfill => maybeBackfill.map(_.priority).getOrElse(0))
    }
    Ordering.by(context => (context.backfill, context.start))
  }
}

case class TimeSeriesDependency(offset: Duration)

case class TimeSeriesScheduling(grid: TimeSeriesGrid, start: Instant, maxPeriods: Int = 1) extends Scheduling {
  type Context = TimeSeriesContext
  type DependencyDescriptor = TimeSeriesDependency
}

object TimeSeriesScheduling {
  implicit def scheduler = TimeSeriesScheduler()
}

case class TimeSeriesScheduler() extends Scheduler[TimeSeriesScheduling] with TimeSeriesApp {
  import TimeSeriesUtils._

  val allContexts = Database.sqlGetContextsBetween(None, None)

  private val timer =
    Job("timer", TimeSeriesScheduling(Continuous, Instant.ofEpochMilli(0)))(_ => sys.error("panic!"))

  private val _state = Ref(Map.empty[TimeSeriesJob, IntervalSet[Instant]])
  private val _backfills = TSet.empty[Backfill]
  private val _running = Ref(Map.empty[TimeSeriesJob, IntervalSet[Instant]])

  private[timeseries] def state: (State, State, Set[Backfill]) = atomic { implicit txn =>
    (_state(), _running(), _backfills.snapshot)
  }

  private[timeseries] def backfillJob(id: String, job: TimeSeriesJob, start: Instant, end: Instant, priority: Int) =
    atomic { implicit txn =>
      val newBackfill = Backfill(id, start, end, Set(job), priority)
      _backfills += newBackfill
      _state() = _state() + (job -> (_state().apply(job) - Interval.closedOpen(start, end)))
    }

  private def backfillDomain(backfill: Backfill) =
    backfill.jobs.map(job => job -> IntervalSet(Interval.closedOpen(backfill.start, backfill.end))).toMap

  def start(workflow: Workflow[TimeSeriesScheduling], executor: Executor[TimeSeriesScheduling], xa: XA): Unit = {
    Database.doSchemaUpdates.transact(xa).unsafePerformIO

    Database
      .deserialize(workflow.vertices)
      .transact(xa)
      .unsafePerformIO
      .foreach {
        case (state, backfillState) =>
          atomic { implicit txn =>
            _state() = _state() ++ state
            _backfills ++= backfillState
          }
      }

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
      val (stateSnapshot, backfillSnapshot, toRun) = atomic { implicit txn =>
        addRuns(completed)
        _backfills.retain { bf =>
          without(StateD(backfillDomain(bf)), StateD(_state())) =!= zero[StateD]
        }
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
        (_state(), _backfills.snapshot, _toRun)
      }

      if (toRun.nonEmpty)
        Database.serialize(stateSnapshot, backfillSnapshot).transact(xa).unsafePerformIO

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

  private[timeseries] def next(workflow0: Workflow[TimeSeriesScheduling],
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

    val _dependencies =
      (for {
        (child, parent, TimeSeriesDependency(offset)) <- workflow.edges.toList
        is = state(parent)
      } yield StateD(Map(child -> is.map(itvl => itvl.map(_.plus(offset)))), IntervalSet(Interval.full)))

    val dependencies = _dependencies.reduce(and(_, _))

    val jobDomain = StateD(
      workflow.vertices.map(job => job -> IntervalSet(Interval.atLeast(job.scheduling.start))).toMap)

    val ready = Seq(complement(runningIntervals), complement(StateD(state)), jobDomain, dependencies)
      .reduce(and(_, _))

    val toBackfill: Map[Backfill, StateD] =
      backfills.map { backfill =>
        backfill -> and(ready, StateD(backfillDomain(backfill)))
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
  type TimeSeriesJob = Job[TimeSeriesScheduling]
  type State = Map[TimeSeriesJob, IntervalSet[Instant]]
  type Executable = (TimeSeriesJob, TimeSeriesContext)
  type Run = (TimeSeriesJob, TimeSeriesContext, Future[Unit])

  val UTC: ZoneId = ZoneId.of("UTC")

  val emptyIntervalSet: IntervalSet[Instant] = IntervalSet.empty[Instant]

  implicit val dateTimeOrdering: Ordering[Instant] =
    Ordering.fromLessThan((t1: Instant, t2: Instant) => t1.isBefore(t2))
}
