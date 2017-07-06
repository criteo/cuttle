package com.criteo.cuttle.timeseries

import TimeSeriesUtils._
import com.criteo.cuttle._
import lol.http._
import lol.json._
import io.circe._
import io.circe.syntax._

import scala.util.Try
import scala.math.Ordering.Implicits._
import java.time.Instant
import java.time.temporal.ChronoUnit._

import doobie.imports._
import intervals._
import Bound.{Bottom, Finite, Top}
import ExecutionStatus._
import com.criteo.cuttle.authentication.AuthenticatedService

private[timeseries] trait TimeSeriesApp { self: TimeSeriesScheduler =>

  import App._
  import TimeSeriesGrid._
  import JobState._

  private implicit val intervalEncoder = new Encoder[Interval[Instant]] {
    implicit val boundEncoder = new Encoder[Bound[Instant]] {
      override def apply(bound: Bound[Instant]) = bound match {
        case Bottom => "-oo".asJson
        case Top => "+oo".asJson
        case Finite(t) => t.asJson
      }
    }

    override def apply(interval: Interval[Instant]) =
      Json.obj(
        "start" -> interval.lo.asJson,
        "end" -> interval.hi.asJson
      )
  }

  private[cuttle] override def publicRoutes(workflow: Workflow[TimeSeries],
                                            executor: Executor[TimeSeries],
                                            xa: XA): PartialService = {

    case request @ GET at url"/api/timeseries/executions?job=$jobId&start=$start&end=$end" =>
      def watchState() = Some((state, executor.allFailing))

      def getExecutions(watchedValue: Any = ()) = {
        val job = workflow.vertices.find(_.id == jobId).get
        val grid = job.scheduling.grid
        val startDate = Instant.parse(start)
        val endDate = Instant.parse(end)
        val requestedInterval = Interval(startDate, endDate)
        val contextQuery = Database.sqlGetContextsBetween(Some(startDate), Some(endDate))
        val archivedExecutions = executor.archivedExecutions(contextQuery, Set(jobId), "", true, 0, Int.MaxValue)
        val runningExecutions = executor.runningExecutions
          .filter {
            case (e, _) =>
              e.job.id == jobId && e.context.toInterval.intersects(requestedInterval)
          }
          .map { case (e, status) => e.toExecutionLog(status) }
        val (state, _) = this.state
        val remainingExecutions =
          for {
            (interval, maybeBackfill) <- state(job)
              .intersect(Interval(startDate, endDate))
              .toList
              .collect { case (itvl, Todo(maybeBackfill)) => (itvl, maybeBackfill) }
            (lo, hi) <- grid.split(interval)
          } yield {
            val context = TimeSeriesContext(grid.truncate(lo), grid.ceil(hi), maybeBackfill)
            ExecutionLog("", job.id, None, None, context.asJson, ExecutionTodo, None)
          }
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
        val period = Interval(startDate, endDate)
        val (state, backfills) = this.state
        val backfillDomain =
          backfills.foldLeft(IntervalMap.empty[Instant, Unit]) { (acc, bf) =>
            if (bf.jobs.map(_.id).intersect(filteredJobs).nonEmpty)
              acc.update(Interval(bf.start, bf.end), ())
            else
              acc
          }
        val allRunning = executor.allRunning
        val allFailing = executor.allFailingExecutions
        val jobTimelines =
          for {
            job <- workflow.vertices
            if filteredJobs.contains(job.id)
            (interval, jobState) <- state(job).intersect(period).toList
            toSplit = jobState match {
              case Running(_) => false
              case _ => true
            }
            (lo, hi) <- {
              if (toSplit) job.scheduling.grid.split(interval)
              else List(interval.toPair)
            }
          } yield {
            val label = jobState match {
              case Running(e) =>
                if (allFailing.exists(_.id == e))
                  "failed"
                else
                  allRunning
                    .find(_.id == e)
                    .map(_.status match {
                      case ExecutionWaiting => "waiting"
                      case ExecutionRunning => "running"
                      case _ => s"unknown"
                    })
                    .getOrElse("unknown")
              case Todo(_) => "todo"
              case Done => "successful"
            }
            val isInBackfill = backfills.exists(
              bf =>
                bf.jobs.contains(job) &&
                  IntervalMap(Interval(lo, hi) -> 0)
                    .intersect(Interval(bf.start, bf.end))
                    .toList
                    .nonEmpty)
            job.id -> ((Interval(lo, hi), label, isInBackfill))
          }
        Json.obj(
          "summary" -> Hourly
            .split(period)
            .map {
              case (lo, hi) =>
                val isInbackfill = backfillDomain.intersect(Interval(lo, hi)).toList.nonEmpty
                val (duration, done, failing) = (for {
                  job <- workflow.vertices
                  if filteredJobs.contains(job.id)
                  (interval, jobState) <- state(job).intersect(Interval(lo, hi)).toList
                } yield {
                  val (lo, hi) = interval.toPair
                  (lo.until(hi, SECONDS), if (jobState == Done) lo.until(hi, SECONDS) else 0, jobState match {
                    case Running(e) => executor.allFailingExecutions.exists(_.id == e)
                    case _ => false
                  })
                }).reduce((a, b) => (a._1 + b._1, a._2 + b._2, a._3 || b._3))
                Interval(lo, hi) ->
                  ((f"${done.toDouble / duration.toDouble}%1.1f", failing, isInbackfill))
            }
            .asJson,
          "jobs" -> jobTimelines.groupBy(_._1).mapValues(_.map(_._2)).asJson
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
        val (state, backfills) = this.state
        val backfillDomain =
          backfills.foldLeft(IntervalMap.empty[Instant, Unit]) { (acc, bf) =>
            if (bf.jobs.map(_.id).intersect(filteredJobs).nonEmpty)
              acc.update(Interval(bf.start, bf.end), ())
            else
              acc
          }
        val upToNow = Interval(Bottom, Finite(Instant.now))
        (for {
          job <- workflow.vertices
          if filteredJobs.contains(job.id)
          (interval, jobState) <- state(job).intersect(upToNow).toList
          (start, end) <- Daily(UTC).split(interval)
        } yield
          (Daily(UTC).truncate(start), start.until(end, SECONDS), jobState == Done, jobState match {
            case Running(exec) => executor.allFailingExecutions.exists(_.id == exec)
            case _ => false
          }))
          .groupBy(_._1)
          .toList
          .map {
            case (date, set) =>
              val (total, done, stuck) = set.foldLeft((0L, 0L, false)) { (acc, exec) =>
                val (totalDuration, doneDuration, isAnyStuck) = acc
                val (_, duration, isDone, isStuck) = exec
                val newDone = if (isDone) duration else 0L
                (totalDuration + duration, doneDuration + newDone, isAnyStuck || isStuck)
              }
              Map(
                "date" -> date.asJson,
                "completion" -> f"${done.toDouble / total.toDouble}%1.1f".asJson
              ) ++ (if (stuck) Map("stuck" -> true.asJson) else Map.empty) ++
                (if (backfillDomain.intersect(Interval(date, Daily(UTC).next(date))).toList.nonEmpty)
                   Map("backfill" -> true.asJson)
                 else Map.empty)
          }
          .asJson

      }

      if (request.headers.get(h"Accept").exists(_ == h"text/event-stream"))
        sse(watchState, getCalendar)
      else
        Ok(getCalendar())

    case GET at url"/api/timeseries/backfills" =>
      Ok(
        Database.queryBackfills.list
          .map(_.map {
            case (id, name, description, jobs, priority, start, end, created_at, status) =>
              Json.obj(
                "id" -> id.asJson,
                "name" -> name.asJson,
                "description" -> description.asJson,
                "jobs" -> jobs.asJson,
                "priority" -> priority.asJson,
                "start" -> start.asJson,
                "end" -> end.asJson,
                "created_at" -> created_at.asJson,
                "status" -> status.asJson
              )
          })
          .transact(xa)
          .unsafePerformIO
          .asJson)
  }

  private[cuttle] override def privateRoutes(workflow: Workflow[TimeSeries],
                                             executor: Executor[TimeSeries],
                                             xa: XA): AuthenticatedService = {
    case POST at url"/api/timeseries/backfill?name=$name&description=$description&jobs=$jobsString&startDate=$start&endDate=$end&priority=$priority" =>
      _ =>
        val jobIds = jobsString.split(",")
        val jobs = workflow.vertices.filter((job: TimeSeriesJob) => jobIds.contains(job.id))
        val startDate = Instant.parse(start)
        val endDate = Instant.parse(end)
        if (backfillJob(name, description, jobs, startDate, endDate, priority.toInt, xa))
          Ok("ok".asJson)
        else
          BadRequest("invalid backfill")
  }
}
