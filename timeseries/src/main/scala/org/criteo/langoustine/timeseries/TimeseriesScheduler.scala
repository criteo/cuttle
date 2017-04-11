package org.criteo.langoustine.timeseries

import org.criteo.langoustine._

import scala.concurrent._
import scala.concurrent.duration.{Duration => ScalaDuration}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.stm._

import java.time._
import java.time.temporal.ChronoUnit._
import java.time.temporal._

import continuum.{Interval, IntervalSet}
import continuum.bound._

import scala.math.Ordering.Implicits._

sealed trait TimeSeriesGrid
case object Hourly extends TimeSeriesGrid
case class Daily(tz: ZoneId) extends TimeSeriesGrid
private case object Continuous extends TimeSeriesGrid

case class TimeSeriesContext(start: LocalDateTime, end: LocalDateTime) extends SchedulingContext
object TimeSeriesContext {
  implicit val ordering: Ordering[TimeSeriesContext] = Ordering.fromLessThan((a, b) => a.start.isBefore(b.start))
}

case class TimeSeriesDependency(offset: Duration)

case class TimeSeriesScheduling(grid: TimeSeriesGrid, start: LocalDateTime) extends Scheduling {
  type Context = TimeSeriesContext
  type DependencyDescriptor = TimeSeriesDependency
}

object TimeSeriesScheduling {
  implicit def scheduler = TimeSeriesScheduler()
}

case class TimeSeriesScheduler() extends Scheduler[TimeSeriesScheduling] with TimeSeriesApp {
  implicit val dateTimeOrdering =
    Ordering.fromLessThan((t1: LocalDateTime, t2: LocalDateTime) => t1.isBefore(t2))

  type TimeSeriesJob = Job[TimeSeriesScheduling]
  type State = Map[Job[TimeSeriesScheduling], IntervalSet[LocalDateTime]]

  val UTC = ZoneId.of("UTC")
  val empty = IntervalSet.empty[LocalDateTime]
  val full = IntervalSet(Interval.full[LocalDateTime])
  val timer = Job("timer", TimeSeriesScheduling(Continuous, LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC)))(_ =>
    sys.error("panic!"))
  val state = TMap.empty[TimeSeriesJob, IntervalSet[LocalDateTime]]

  def intervalFromContext(context: TimeSeriesContext): Interval[LocalDateTime] =
    Interval.closedOpen(context.start, context.end)

  def run(graph: Graph[TimeSeriesScheduling], executor: Executor[TimeSeriesScheduling]) = {
    atomic { implicit txn =>
      graph.vertices.foreach { job =>
        if (!state.contains(job)) {
          state += (job -> IntervalSet.empty[LocalDateTime])
        }
      }
    }

    def addRuns(runs: Set[(TimeSeriesJob, TimeSeriesContext, Future[Unit])])(implicit txn: InTxn) =
      runs.foreach {
        case (job, context, _) =>
          state.update(job, state.get(job).getOrElse(empty) + intervalFromContext(context))
      }

    def go(running: Set[(TimeSeriesJob, TimeSeriesContext, Future[Unit])]): Unit = {
      val (done, stillRunning) = running.partition(_._3.isCompleted)
      val stateSnapshot = atomic { implicit txn =>
        addRuns(done)
        state.snapshot
      }
      val toRun = next(
        graph,
        stateSnapshot,
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

  def split(start: LocalDateTime, end: LocalDateTime, tz: ZoneId, unit: ChronoUnit) = {
    val List(zonedStart, zonedEnd) = List(start, end).map { t =>
      t.atZone(UTC).withZoneSameInstant(tz)
    }
    val truncatedStart = zonedStart.truncatedTo(unit)
    val alignedStart =
      if (truncatedStart == zonedStart) zonedStart
      else truncatedStart.plus(1, unit)
    val alignedEnd = zonedEnd.truncatedTo(unit)
    val periods = alignedStart.until(alignedEnd, unit)
    (1L to periods).map { i =>
      def alignedNth(k: Long) =
        alignedStart
          .plus(k, unit)
          .withZoneSameInstant(UTC)
          .toLocalDateTime
      TimeSeriesContext(alignedNth(i - 1), alignedNth(i))
    }
  }

  def next(graph0: Graph[TimeSeriesScheduling],
           state0: State,
           running: Set[(TimeSeriesJob, TimeSeriesContext)],
           timerInterval: IntervalSet[LocalDateTime]): List[(TimeSeriesJob, TimeSeriesContext)] = {
    val graph = graph0 dependsOn timer
    val state = state0 + (timer -> timerInterval)
    val runningIntervals = running
      .groupBy(_._1)
      .map { case (job, runs) => job -> runs.map(x => intervalFromContext(x._2)) }
    val domain = state
      .map {
        case (job, is) =>
          job -> runningIntervals.getOrElse(job, List.empty).foldLeft(is)(_ + _)
      }
      .map { case (job, intervalSet) => job -> intervalSet.complement }
    val ready = graph.edges
      .foldLeft(domain) {
        case (partial, (jobLeft, jobRight, TimeSeriesDependency(offset))) =>
          partial + (jobLeft ->
            partial(jobLeft)
              .intersect(state(jobRight))
              .map(i => i.map(_.plus(offset))))
      }
      .map {
        case (job, intervalSet) =>
          job -> intervalSet.intersect(Interval.atLeast(job.scheduling.start))
      }
    val res = ready
      .filterNot { case (job, _) => job == timer }
      .map {
        case (job, intervalSet) =>
          val contexts = intervalSet.toList.flatMap { interval =>
            val (unit, tz) = job.scheduling.grid match {
              case Hourly => (HOURS, UTC)
              case Daily(tz) => (DAYS, tz)
              case Continuous => sys.error("panic!")
            }
            val Closed(start) = interval.lower.bound
            val Open(end) = interval.upper.bound
            split(start, end, tz, unit)
          }
          job -> contexts
      }
    res.toList.flatMap { case (job, l) => l.map(i => (job, i)) }
  }
}
