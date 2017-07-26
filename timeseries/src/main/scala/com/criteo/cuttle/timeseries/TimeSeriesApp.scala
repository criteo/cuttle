package com.criteo.cuttle.timeseries

import TimeSeriesUtils._
import com.criteo.cuttle._
import lol.http._
import lol.json._
import io.circe._
import io.circe.syntax._
import io.circe.generic.auto._
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

  trait ExecutionPeriod {
    val period: Interval[Instant]
    val backfill: Boolean
    val aggregated: Boolean
  }

  case class JobExecution(period: Interval[Instant], status: String, backfill: Boolean) extends ExecutionPeriod {
    override val aggregated: Boolean = false
  }

  case class AggregatedJobExecution(period: Interval[Instant], completion: String, error: Boolean, backfill: Boolean) extends ExecutionPeriod {
    override val aggregated: Boolean = true
  }

  case class JobTimeline(jobId: String, gridView: TimeSeriesGridView, executions: List[ExecutionPeriod])

  private implicit val executionPeriodEncoder = new Encoder[ExecutionPeriod] {
    override def apply(executionPeriod: ExecutionPeriod) = {
      val coreFields = List(
        "period" -> executionPeriod.period.asJson,
        "backfill" -> executionPeriod.backfill.asJson,
        "aggregated" -> executionPeriod.aggregated.asJson
      )
      val finalFields = executionPeriod match {
        case JobExecution(_, status, _) => ("status" -> status.asJson) :: coreFields
        case AggregatedJobExecution(_, completion, error, _) => ("completion" -> completion.asJson) :: ("error" -> error.asJson) :: coreFields
      }
      Json.obj(finalFields:_*)
    }
  }

  private implicit val jobTimelineEncoder = new Encoder[JobTimeline] {
    override def apply(jobTimeline: JobTimeline) = jobTimeline.executions.asJson
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
            ExecutionLog("", job.id, None, None, context.asJson, ExecutionTodo, None, 0)
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

        val allFailing = executor.allFailingExecutions

        def findAggregationLevel(n: Int,
                                 gridView: TimeSeriesGridView,
                                 interval: Interval[Instant]): TimeSeriesGridView = {
          val aggregatedExecutions = gridView.split(interval)
          if (aggregatedExecutions.size <= n)
            gridView
          else
            findAggregationLevel(n, gridView.upper(), interval)
        }

        def aggregateExecutions(job: TimeSeriesJob,
                                period: Interval[Instant],
                                gridView: TimeSeriesGridView): List[(Interval[Instant], List[(Interval[Instant], JobState)])] = {
          gridView
            .split(period)
            .map { interval =>
              {
                val (start, end) = interval
                val currentlyAggregatedPeriod = state(job)
                  .intersect(Interval(start, end))
                  .toList
                  .sortBy(_._1.lo)
                currentlyAggregatedPeriod match {
                  case Nil => None
                  case _ => Some((Interval(start, end), currentlyAggregatedPeriod))
                }
              }
            }.flatten
        }

        def getStatusLabelFromState(jobState: JobState): String =
          jobState match {
            case Running(e) =>
              if (allFailing.exists(_.id == e))
                "failed"
              else if (executor.platforms.exists(_.waiting.exists(_.id == e)))
                "waiting"
              else "running"
            case Todo(_) => "todo"
            case Done => "successful"
          }

        val jobTimelines =
          (for { job <- workflow.vertices if filteredJobs.contains(job.id) } yield {
            val gridView = findAggregationLevel(
              48,
              TimeSeriesGridView(job.scheduling.grid),
              period
            )
            val jobExecutions = (for {
                (interval, jobStatesOnIntervals) <- aggregateExecutions(job, period, gridView)
              } yield {
              val (intervalStart, intervalEnd) = interval.toPair
              val inBackfill = backfills.exists(
                bf =>
                  bf.jobs.contains(job) &&
                    IntervalMap(interval -> 0)
                      .intersect(Interval(bf.start, bf.end))
                      .toList
                      .nonEmpty)
              if (gridView.aggregationFactor == 1)
                jobStatesOnIntervals match {
                  case (executionInterval, state) :: Nil => Some(JobExecution(interval, getStatusLabelFromState(state), inBackfill))
                  case _ => None
                }
              else jobStatesOnIntervals match {
                case l => {
                  val (duration, done, error) = l.foldLeft((0L, 0L, false))((acc, currentExecution) => {
                    val (lo, hi) = currentExecution._1.toPair
                    val jobStatus = getStatusLabelFromState(currentExecution._2)
                    (acc._1 + lo.until(hi, SECONDS), acc._2 + (if (jobStatus == "successful") lo.until(hi, SECONDS) else 0), acc._3 || jobStatus == "failed")
                  })
                  Some (AggregatedJobExecution (interval, f"${done.toDouble / duration.toDouble}%2.2f", error, inBackfill))
                }
                case Nil => None
              }
            })
            JobTimeline(job.id, gridView, jobExecutions.flatten)
          }).toList

        Json.obj(
          "summary" -> jobTimelines
            .maxBy(_.executions.size)
            .gridView
            .split(period)
            .flatMap {
              case (lo, hi) =>
                val isInbackfill = backfillDomain.intersect(Interval(lo, hi)).toList.nonEmpty
                val jobSummaries = for {
                  job <- workflow.vertices
                  if filteredJobs.contains(job.id)
                  (interval, jobState) <- state(job).intersect(Interval(lo, hi)).toList
                } yield {
                  val (lo, hi) = interval.toPair
                  (lo.until(hi, SECONDS), if (jobState == Done) lo.until(hi, SECONDS) else 0, jobState match {
                    case Running(e) => allFailing.exists(_.id == e)
                    case _ => false
                  })
                }
                if (jobSummaries.nonEmpty) {
                  val (duration, done, failing) = jobSummaries
                    .reduce((a, b) => (a._1 + b._1, a._2 + b._2, a._3 || b._3))
                  Some(
                    AggregatedJobExecution(Interval(lo, hi), f"${done.toDouble / duration.toDouble}%2.2f", failing, isInbackfill)
                  )
                } else {
                  None
                }
            }
            .asJson,
          "jobs" -> jobTimelines.map(jt => (jt.jobId -> jt)).toMap.asJson
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
          .sortBy(_._1)
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
        Database.queryBackfills().list
          .map(_.map {
            case (id, name, description, jobs, priority, start, end, created_at, status, created_by) =>
              Json.obj(
                "id" -> id.asJson,
                "name" -> name.asJson,
                "description" -> description.asJson,
                "jobs" -> jobs.asJson,
                "priority" -> priority.asJson,
                "start" -> start.asJson,
                "end" -> end.asJson,
                "created_at" -> created_at.asJson,
                "status" -> status.asJson,
                "created_by" -> created_by.asJson
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
      implicit user_ =>
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
