package com.criteo.cuttle.timeseries

import TimeSeriesUtils._

import com.criteo.cuttle._

import lol.http._
import lol.json._

import io.circe._
import io.circe.syntax._

import algebra.lattice.Bool._

import scala.util.{Try}
import scala.math.Ordering.Implicits._

import continuum._
import continuum.bound._

import java.time.{Instant, ZoneId}
import java.time.temporal.ChronoUnit._
import java.util.UUID

import ExecutionStatus._

private[timeseries] trait TimeSeriesApp { self: TimeSeriesScheduler =>
  import App._

  private implicit val intervalEncoder = new Encoder[Interval[Instant]] {
    override def apply(interval: Interval[Instant]) = {
      val Closed(start) = interval.lower.bound
      val Open(end) = interval.upper.bound
      Json.obj(
        "start" -> start.asJson,
        "end" -> end.asJson
      )
    }
  }

  private[cuttle] override def routes(workflow: Workflow[TimeSeries],
                                      executor: Executor[TimeSeries],
                                      xa: XA): PartialService = {

    case request @ GET at url"/api/timeseries/executions?job=$jobId&start=$start&end=$end" =>
      def watchState() = Some((state, executor.allFailing))
      def getExecutions(watchedValue: Any = ()) = {
        val job = workflow.vertices.find(_.id == jobId).get
        val startDate = Instant.parse(start)
        val endDate = Instant.parse(end)
        val requestedInterval = Interval.closedOpen(startDate, endDate)
        val contextQuery = Database.sqlGetContextsBetween(Some(startDate), Some(endDate))
        val archivedExecutions = executor.archivedExecutions(contextQuery, Set(jobId), "", true, 0, Int.MaxValue)
        val runningExecutions = executor.runningExecutions
          .filter {
            case (e, _) =>
              e.job.id == jobId && e.context.toInterval.encloses(requestedInterval)
          }
          .map { case (e, status) => e.toExecutionLog(status) }
        val (done, running, _) = state
        val scheduledPeriods = done(job) ++ running(job)
        val remainingPeriods =
          scheduledPeriods.complement.intersect(Interval.atLeast(job.scheduling.start)).intersect(requestedInterval)
        val remainingContexts: Seq[TimeSeriesContext] = for {
          interval <- remainingPeriods.toSeq
          context <- splitInterval(job, interval, false)
        } yield context
        val remainingExecutions =
          remainingContexts.map(ctx => ExecutionLog("", job.id, None, None, ctx.asJson, ExecutionTodo, None))
        val throttledExecutions = executor.allFailingExecutions
          .filter(e => e.job == job && e.context.toInterval.intersects(requestedInterval))
          .map(_.toExecutionLog(ExecutionThrottled))
        (archivedExecutions ++ runningExecutions ++ remainingExecutions ++ throttledExecutions).asJson
      }
      if (request.headers.get(h"Accept").exists(_ == h"text/event-stream"))
        sse(watchState, getExecutions)
      else
        Ok(getExecutions())

    case request @ GET at url"/api/timeseries/calendar/focus?start=$start&end=$end&jobs=$jobs" =>
      val filteredJobs = Try(jobs.split(",").toSeq.filter(_.nonEmpty)).toOption
        .filter(_.nonEmpty)
        .getOrElse(workflow.vertices.map(_.id))
        .toSet
      def watchState() = Some((state, executor.allFailing))
      def getFocusView(watchedValue: Any = ()) = {
        val startDate = Instant.parse(start)
        val endDate = Instant.parse(end)
        val (_done, _running, backfills) = state
        val filteredBackfills = backfills.filter(bf => (bf.jobs.map(_.id) & filteredJobs) != Set.empty[String])
        val stucks = executor.allFailing
        val periodInterval = Interval.closedOpen(startDate, endDate)
        val period = StateD(Map.empty, IntervalSet(periodInterval))
        val done = and(StateD(_done), period)
        val running = and(StateD(_running), period)
        val domain = StateD(
          workflow.vertices.map(job => job -> IntervalSet(Interval.atLeast(job.scheduling.start))).toMap)
        val remaining = List(complement(or(done, running)), period, domain).reduce(and[StateD])
        val waiting = executor.platforms.flatMap(_.waiting)
        val executions = List((done, "successful"), (running, "running"), (remaining, "todo"))
          .flatMap {
            case (state, label) =>
              for {
                (job, is) <- state.defined.toList
                if filteredJobs.contains(job.id)
                interval <- is
                context <- splitInterval(job, interval, false).toList
              } yield {
                val isInBackfill = filteredBackfills.exists(
                  backfill =>
                    backfill.jobs.contains(job) &&
                      context.toInterval.intersects(Interval.closedOpen(backfill.start, backfill.end)))
                val lbl =
                  if (label != "running")
                    label
                  else if (stucks.exists {
                             case (stuckJob, stuckCtx) =>
                               stuckCtx.toInterval.intersects(context.toInterval) && stuckJob == job
                           })
                    "failed"
                  else if (waiting.exists { e =>
                             e.context match {
                               case executionContext: TimeSeriesContext =>
                                 e.job == job && executionContext.toInterval.intersects(context.toInterval)
                               case _ => false
                             }
                           })
                    "waiting"
                  else
                    "running"
                job.id -> ((context.toInterval.intersect(periodInterval).get, lbl, isInBackfill))
              }
          }
        val jobTimelines = executions
          .groupBy(_._1)
          .mapValues(_.map(_._2).sortBy(_._1))
        val summary =
          (for {
            (_, (interval, label, _)) <- executions
            Closed(start) = interval.lower.bound
            Open(end) = interval.upper.bound
            duration = start.until(end, HOURS)
            hour <- (1L to duration).map(i => Interval.closedOpen(start.plus(i - 1, HOURS), start.plus(i, HOURS)))
          } yield (hour, label))
            .groupBy(_._1)
            .map {
              case (hour, xs) =>
                hour ->
                  ((xs.filter(_._2 == "successful").length.toFloat / xs.length,
                    xs.exists(_._2 == "failed"),
                    filteredBackfills.exists(backfill =>
                      hour.intersects(Interval.closedOpen(backfill.start, backfill.end)))))
            }
            .toList
            .sortBy(_._1)
        Json.obj(
          "summary" -> summary.asJson,
          "jobs" -> jobTimelines.asJson
        )
      }
      if (request.headers.get(h"Accept").exists(_ == h"text/event-stream"))
        sse(watchState, getFocusView)
      else
        Ok(getFocusView())

    case request @ GET at url"/api/timeseries/calendar?jobs=$jobs" =>
      val filteredJobs = Try(jobs.split(",").toSeq.filter(_.nonEmpty)).toOption
        .filter(_.nonEmpty)
        .getOrElse(workflow.vertices.map(_.id))
        .toSet
      def watchState() = Some((state, executor.allFailing))
      def getCalendar(watchedValue: Any = ()) = {
        val (done, running, backfills) = state match {
          case (done, running, backfills) =>
            val filter = (job: TimeSeriesJob) => filteredJobs.contains(job.id)
            (done.filterKeys(filter),
             running.filterKeys(filter),
             backfills.filter(bf => (filteredJobs & bf.jobs.map(_.id)) != Set.empty[String]))
        }
        val stucks = executor.allFailing.filter(x => filteredJobs.contains(x._1.id))
        val goal = StateD(
          workflow.vertices
            .filter(job => filteredJobs.contains(job.id))
            .map(job => (job -> IntervalSet(Interval.atLeast(job.scheduling.start))))
            .toMap)
        val domain = and(or(StateD(done), StateD(running)), goal).defined
        val backfillDays = backfills.flatMap { backfill =>
          val startDate = backfill.start.atZone(ZoneId.of("UTC")).toLocalDate
          val endDate = {
            val d = backfill.end.atZone(ZoneId.of("UTC")).toLocalDate
            if (d.atStartOfDay(ZoneId.of("UTC")) == backfill.end.atZone(ZoneId.of("UTC")))
              d.minus(1, DAYS)
            else d
          }
          val duration = startDate.until(endDate, DAYS)
          (0L to duration).toList.map(i => startDate.plus(i, DAYS))
        }
        val days = (for {
          (_, is) <- domain.toList
          interval <- is.toList
          Closed(start) = interval.lower.bound
          Open(end) = interval.upper.bound
          startDate = start.atZone(ZoneId.of("UTC")).toLocalDate
          endDate = {
            val d = end.atZone(ZoneId.of("UTC")).toLocalDate
            if (d.atStartOfDay(ZoneId.of("UTC")) == end.atZone(ZoneId.of("UTC")))
              d.minus(1, DAYS)
            else d
          }
          duration = startDate.until(endDate, DAYS)
          day <- (0L to duration).toList.map(i => startDate.plus(i, DAYS))
        } yield day).toSet
        val calendar = days.map { d =>
          val start = d.atStartOfDay(ZoneId.of("UTC")).toInstant
          val day = Interval.closedOpen(start, start.plus(1, DAYS))
          val doneForDay =
            and(StateD(done), StateD(Map.empty, IntervalSet(day)))
          val goalForDay = and(goal, StateD(Map.empty, IntervalSet(day)))
          val isStuck = stucks.exists(_._2.toInterval.intersects(day))
          val isInBackfill = backfillDays.contains(d)

          def totalDuration(state: StateD) =
            (for {
              (_, is) <- state.defined.toList
              interval <- is
            } yield {
              val Closed(start) = interval.lower.bound
              val Open(end) = interval.upper.bound
              start.until(end, SECONDS)
            }).fold(0L)(_ + _)
          (d.toString,
           f"${totalDuration(doneForDay).toDouble / totalDuration(goalForDay)}%1.1f",
           isStuck,
           isInBackfill)
        }
        calendar.toList
          .sortBy(_._1)
          .map {
            case (date, completion, isStuck, isInBackfill) =>
              (Map(
                "date" -> date.asJson,
                "completion" -> completion.asJson
              ) ++ (if (isStuck) Map("stuck" -> true.asJson) else Map.empty)
                ++ (if (isInBackfill) Map("backfill" -> true.asJson) else Map.empty)).asJson
          }
          .asJson
      }
      if (request.headers.get(h"Accept").exists(_ == h"text/event-stream"))
        sse(watchState, getCalendar)
      else
        Ok(getCalendar())

    case POST at url"/api/timeseries/backfill?jobs=$jobsString&startDate=$start&endDate=$end&priority=$priority" =>
      val jobIds = jobsString.split(",")
      val jobs = this.state._1.keySet.filter((job: TimeSeriesJob) => jobIds.contains(job.id))
      val startDate = Instant.parse(start)
      val endDate = Instant.parse(end)
      val backfillId = UUID.randomUUID().toString
      backfillJob(backfillId, jobs, startDate, endDate, priority.toInt)
      Ok(
        Json.obj(
          "id" -> backfillId.asJson
        ))
  }
}
