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

case object Hourly extends TimeSeriesGrid

case class Daily(tz: ZoneId) extends TimeSeriesGrid

private case object Continuous extends TimeSeriesGrid

case class Backfill(id: String, start: Instant, end: Instant, jobs: Set[Job[TimeSeriesScheduling]], priority: Int)
object Backfill {
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

  implicit val encoder: Encoder[TimeSeriesContext] = deriveEncoder

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

  def state: (State, Set[Backfill]) = atomic { implicit txn =>
    (_state(), _backfills.snapshot)
  }

  def backfillJob(id: String, job: TimeSeriesJob, start: Instant, end: Instant, priority: Int) = atomic {
    implicit txn =>
      val newBackfill = Backfill(id, start, end, Set(job), priority)
      _backfills += newBackfill
      _state() = _state() + (job -> (_state().apply(job) - Interval.closedOpen(start, end)))
  }

  def backfillDomain(backfill: Backfill) =
    backfill.jobs.map(job => job -> IntervalSet(Interval.closedOpen(backfill.start, backfill.end))).toMap

  def run(graph: Graph[TimeSeriesScheduling], executor: Executor[TimeSeriesScheduling], xa: XA): Unit = {
    Database.doSchemaUpdates.transact(xa).unsafePerformIO

    Database
      .deserialize(graph.vertices)
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
      graph.vertices.foreach { job =>
        if (!_state().contains(job)) {
          _state() = _state() + (job -> IntervalSet.empty[Instant])
        }
      }
    }

    def addRuns(runs: Set[Run])(implicit txn: InTxn) =
      runs.foreach {
        case (job, context, _) =>
          _state() = _state() + (job -> (_state().apply(job) + context.toInterval))
      }

    def go(running: Set[Run]): Unit = {
      val (completed, stillRunning) = running.partition(_._3.isCompleted)
      val done = completed.filter(_._3.value.get.isSuccess)
      val (stateSnapshot, backfillSnapshot) = atomic { implicit txn =>
        addRuns(done)
        _backfills.retain { bf =>
          without(StateD(backfillDomain(bf)), StateD(_state())) =!= zero[StateD]
        }
        (_state(), _backfills.snapshot)
      }

      if (completed.nonEmpty)
        Database.serialize(stateSnapshot, backfillSnapshot).transact(xa).unsafePerformIO

      val now = Instant.now
      val toRun = next(
        graph,
        stateSnapshot,
        backfillSnapshot,
        stillRunning.map { case (job, context, _) => (job, context) },
        IntervalSet(Interval.lessThan(now))
      )
      val newRunning = stillRunning ++ executor.runAll(toRun).map { submitted =>
        (submitted.execution.job, submitted.execution.context, submitted.result)
      }

      Future.firstCompletedOf(utils.Timeout(ScalaDuration.create(1, "s")) :: newRunning.map(_._3).toList).andThen {
        case _ => go(newRunning)
      }
    }

    go(Set.empty)
  }

  def split(start: Instant, end: Instant, tz: ZoneId, unit: ChronoUnit, maxPeriods: Int): Iterator[TimeSeriesContext] = {
    val List(zonedStart, zonedEnd) = List(start, end).map { t =>
      t.atZone(UTC).withZoneSameInstant(tz)
    }

    val truncatedStart = zonedStart.truncatedTo(unit)
    val alignedStart =
      if (truncatedStart == zonedStart)
        zonedStart
      else
        truncatedStart.plus(1, unit)

    val alignedEnd = zonedEnd.truncatedTo(unit)
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

  def next(graph0: Graph[TimeSeriesScheduling],
           state0: State,
           backfills: Set[Backfill],
           running: Set[(TimeSeriesJob, TimeSeriesContext)],
           timerInterval: IntervalSet[Instant]): List[Executable] = {
    val graph = graph0 dependsOn timer
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
        (child, parent, TimeSeriesDependency(offset)) <- graph.edges
        is <- state.get(parent).toList
      } yield StateD(Map(child -> is.map(itvl => itvl.map(_.plus(offset)))), IntervalSet(Interval.full)))
        .reduce(and(_, _))

    val jobDomain = StateD(graph.vertices.map(job => job -> IntervalSet(Interval.atLeast(job.scheduling.start))).toMap)

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

    val splitInterval = (job: TimeSeriesJob, interval: Interval[Instant]) => {
      val (unit, tz) = job.scheduling.grid match {
        case Hourly => (HOURS, UTC)
        case Daily(_tz) => (DAYS, _tz)
        case Continuous => sys.error("panic!")
      }
      val Closed(start) = interval.lower.bound
      val Open(end) = interval.upper.bound
      split(start, end, tz, unit, job.scheduling.maxPeriods)
    }

    for {
      (maybeBackfill, state) <- toRun.toList
      (job, intervalSet) <- state.toList.filterNot { case (job, _) => job == timer }
      interval <- intervalSet
      context <- splitInterval(job, interval)
    } yield (job, context.copy(backfill = maybeBackfill))
  }
}

object TimeSeriesUtils {
  type TimeSeriesJob = Job[TimeSeriesScheduling]
  type State = Map[TimeSeriesJob, IntervalSet[Instant]]
  type Executable = (TimeSeriesJob, TimeSeriesContext)
  type Run = (TimeSeriesJob, TimeSeriesContext, Future[Unit])

  val UTC: ZoneId = ZoneId.of("UTC")

  val emptyIntervalSet: IntervalSet[Instant] = IntervalSet.empty[Instant]

  implicit val dateTimeOrdering: Ordering[Instant] =
    Ordering.fromLessThan((t1: Instant, t2: Instant) => t1.isBefore(t2))
}
