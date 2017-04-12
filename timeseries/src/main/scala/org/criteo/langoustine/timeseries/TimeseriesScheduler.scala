package org.criteo.langoustine.timeseries

import org.criteo.langoustine._

import scala.concurrent._
import scala.concurrent.duration.{Duration => ScalaDuration}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.stm._

import io.circe._

import java.time._
import java.time.temporal.ChronoUnit._
import java.time.temporal._

import continuum.{Interval, IntervalSet}
import continuum.bound._

sealed trait TimeSeriesGrid

case object Hourly extends TimeSeriesGrid

case class Daily(tz: ZoneId) extends TimeSeriesGrid

private case object Continuous extends TimeSeriesGrid

case class Backfill(start: LocalDateTime, end: LocalDateTime, jobs: Set[Job[TimeSeriesScheduling]], priority: Int)

case class TimeSeriesContext(start: LocalDateTime, end: LocalDateTime, backfill: Option[Backfill] = None)
    extends SchedulingContext {
  import TimeSeriesUtils._

  def toJson = Json.obj(
    "start" -> Json.fromLong(start.toEpochSecond(ZoneOffset.of("Z"))),
    "end" -> Json.fromLong(end.toEpochSecond(ZoneOffset.of("Z")))
  )

  def toInterval: Interval[LocalDateTime] = Interval.closedOpen(start, end)
}

object TimeSeriesContext {
  import TimeSeriesUtils._

  implicit val ordering: Ordering[TimeSeriesContext] = {
    implicit val maybeBackfillOrdering: Ordering[Option[Backfill]] = {
      Ordering.by(maybeBackfill => maybeBackfill.map(_.priority).getOrElse(0))
    }
    Ordering.by(context => (context.backfill, context.start))
  }
}

case class TimeSeriesDependency(offset: Duration)

case class TimeSeriesScheduling(grid: TimeSeriesGrid, start: LocalDateTime, maxPeriods: Int = 1) extends Scheduling {
  type Context = TimeSeriesContext
  type DependencyDescriptor = TimeSeriesDependency
}

object TimeSeriesScheduling {
  implicit def scheduler = TimeSeriesScheduler()
}

case class TimeSeriesScheduler() extends Scheduler[TimeSeriesScheduling] with TimeSeriesApp {
  import TimeSeriesUtils._

  type TimeSeriesJob = Job[TimeSeriesScheduling]
  type State = Map[Job[TimeSeriesScheduling], IntervalSet[LocalDateTime]]
  type Executable = (TimeSeriesJob, TimeSeriesContext)
  type Run = (TimeSeriesJob, TimeSeriesContext, Future[Unit])

  private val timer =
    Job("timer", None, None, Set(), TimeSeriesScheduling(Continuous, LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC)))(_ =>
      sys.error("panic!"))

  private val _state = TMap.empty[TimeSeriesJob, IntervalSet[LocalDateTime]]
  private val _backfills = TMap.empty[Backfill, State]

  def state: (State, Map[Backfill, State]) = atomic { implicit txn =>
    (_state.snapshot, _backfills.snapshot)
  }

  def run(graph: Graph[TimeSeriesScheduling], executor: Executor[TimeSeriesScheduling]): Unit = {
    atomic { implicit txn =>
      graph.vertices.foreach { job =>
        if (!_state.contains(job)) {
          _state += (job -> IntervalSet.empty[LocalDateTime])
        }
      }
    }

    def addRuns(runs: Set[Run])(implicit txn: InTxn) =
      runs.foreach {
        case (job, context, _) =>
          _state.update(job, _state.get(job).getOrElse(emptyIntervalSet) + context.toInterval)
          context.backfill.foreach { backfill =>
            val backfillState: State = _backfills(backfill)
            val newBackfillState = backfillState + (job ->
              (backfillState.get(job).getOrElse(emptyIntervalSet) - context.toInterval))
            _backfills.update(backfill, newBackfillState)
          }
      }

    def go(running: Set[Run]): Unit = {
      val (done, stillRunning) = running.partition(_._3.isCompleted)
      val (stateSnapshot, backfillSnapshot) = atomic { implicit txn =>
        addRuns(done)
        _backfills.retain { case (_, state) => !state.forall(_._2.isEmpty) }
        (_state.snapshot, _backfills.snapshot)
      }
      val toRun = next(
        graph,
        stateSnapshot,
        backfillSnapshot,
        stillRunning.map { case (job, context, _) => (job, context) },
        IntervalSet(Interval.lessThan(LocalDateTime.now))
      )
      val newRunning = stillRunning ++ toRun.map {
        case (job, context) =>
          (job, context, executor.run(job, context))
      }
      Future.firstCompletedOf(utils.Timeout(ScalaDuration.create(1, "s")) :: newRunning.map(_._3).toList).andThen {
        case _ => go(newRunning)
      }
    }

    go(Set.empty)
  }

  def split(start: LocalDateTime,
            end: LocalDateTime,
            tz: ZoneId,
            unit: ChronoUnit,
            maxPeriods: Int): Iterator[TimeSeriesContext] = {
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
          .toLocalDateTime

      TimeSeriesContext(alignedNth(l.head), alignedNth(l.last + 1))
    }
  }

  def next(graph0: Graph[TimeSeriesScheduling],
           state0: State,
           backfills: Map[Backfill, State],
           running: Set[(TimeSeriesJob, TimeSeriesContext)],
           timerInterval: IntervalSet[LocalDateTime]): List[Executable] = {
    val graph = graph0 dependsOn timer
    val state = state0 + (timer -> timerInterval)

    val runningIntervals = running
      .groupBy(_._1)
      .map { case (job, runs) => job -> runs.map(x => x._2.toInterval) }

    val domain = state
      .map { case (job, is) => job -> runningIntervals.getOrElse(job, List.empty).foldLeft(is)(_ + _) }
      .map { case (job, is) => job -> is.complement }

    val ready = graph.edges
      .foldLeft(domain) {
        case (partial, (jobLeft, jobRight, TimeSeriesDependency(offset))) =>
          val runningDependency = jobRight ->
            (partial(jobRight) -- runningIntervals.getOrElse(jobLeft, emptyIntervalSet))
              .map(i => i.map(_.minus(offset)))
          val dependency = jobLeft ->
            partial(jobLeft)
              .intersect(state(jobRight))
              .map(i => i.map(_.plus(offset)))
          partial + runningDependency + dependency
      }
      .map {
        case (job, intervalSet) =>
          job -> intervalSet.intersect(Interval.atLeast(job.scheduling.start))
      }

    val res = ready
      .filterNot { case (job, _) => job == timer }
      .map {
        case (job, intervalSet) =>
          val backfilledIntervals =
            backfills.toList.flatMap(_._2.getOrElse(job, emptyIntervalSet)).toSet
          val toRun = intervalSet -- backfilledIntervals

          val intervalsForBackfills = backfills
            .map { case (key, st) => (key, st.getOrElse(job, emptyIntervalSet)) }
            .toList
            .flatMap { case (k, is) => is.map((k, _)) }
            .map { case (backfill, interval) => (interval, Some(backfill)) }

          val intervals: List[(Interval[LocalDateTime], Option[Backfill])] =
            toRun.toList.map((_, None)) ++ intervalsForBackfills

          val contexts = intervals.flatMap {
            case (interval, maybeBackfill) =>
              val (unit, tz) = job.scheduling.grid match {
                case Hourly => (HOURS, UTC)
                case Daily(_tz) => (DAYS, _tz)
                case Continuous => sys.error("panic!")
              }
              val Closed(start) = interval.lower.bound
              val Open(end) = interval.upper.bound
              split(start, end, tz, unit, job.scheduling.maxPeriods)
                .map(_.copy(backfill = maybeBackfill))
          }
          job -> contexts
      }

    res.toList.flatMap { case (job, l) => l.map(i => (job, i)) }
  }
}

object TimeSeriesUtils {
  val UTC: ZoneId = ZoneId.of("UTC")

  val emptyIntervalSet: IntervalSet[LocalDateTime] = IntervalSet.empty[LocalDateTime]

  implicit val dateTimeOrdering: Ordering[LocalDateTime] =
    Ordering.fromLessThan((t1: LocalDateTime, t2: LocalDateTime) => t1.isBefore(t2))
}
