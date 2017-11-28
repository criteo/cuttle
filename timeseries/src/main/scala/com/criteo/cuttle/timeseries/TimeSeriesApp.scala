package com.criteo.cuttle.timeseries

import TimeSeriesUtils._
import com.criteo.cuttle._
import lol.http._
import lol.json._
import io.circe._
import io.circe.syntax._
import io.circe.generic.auto._

import scala.util.Try
import java.time.Instant
import java.time.temporal.ChronoUnit._

import doobie.implicits._
import intervals._
import Bound.{Bottom, Finite, Top}
import ExecutionStatus._
import Auth._
import cats.effect.IO
import cats.implicits._

private[timeseries] trait TimeSeriesApp { self: TimeSeriesScheduler =>

  import App._
  import TimeSeriesCalendar._
  import JobState._

  private implicit val intervalEncoder = new Encoder[Interval[Instant]] {
    implicit val boundEncoder = new Encoder[Bound[Instant]] {
      override def apply(bound: Bound[Instant]) = bound match {
        case Bottom    => "-oo".asJson
        case Top       => "+oo".asJson
        case Finite(t) => t.asJson
      }
    }

    override def apply(interval: Interval[Instant]) =
      Json.obj(
        "start" -> interval.lo.asJson,
        "end" -> interval.hi.asJson
      )
  }

  private trait ExecutionPeriod {
    val period: Interval[Instant]
    val backfill: Boolean
    val aggregated: Boolean
  }

  private case class JobExecution(period: Interval[Instant], status: String, backfill: Boolean)
      extends ExecutionPeriod {
    override val aggregated: Boolean = false
  }

  private case class AggregatedJobExecution(period: Interval[Instant],
                                            completion: String,
                                            error: Boolean,
                                            backfill: Boolean)
      extends ExecutionPeriod {
    override val aggregated: Boolean = true
  }

  private case class JobTimeline(jobId: String, calendarView: TimeSeriesCalendarView, executions: List[ExecutionPeriod])

  private implicit val executionPeriodEncoder = new Encoder[ExecutionPeriod] {
    override def apply(executionPeriod: ExecutionPeriod) = {
      val coreFields = List(
        "period" -> executionPeriod.period.asJson,
        "backfill" -> executionPeriod.backfill.asJson,
        "aggregated" -> executionPeriod.aggregated.asJson
      )
      val finalFields = executionPeriod match {
        case JobExecution(_, status, _) => ("status" -> status.asJson) :: coreFields
        case AggregatedJobExecution(_, completion, error, _) =>
          ("completion" -> completion.asJson) :: ("error" -> error.asJson) :: coreFields
      }
      Json.obj(finalFields: _*)
    }
  }

  private implicit val jobTimelineEncoder = new Encoder[JobTimeline] {
    override def apply(jobTimeline: JobTimeline) = jobTimeline.executions.asJson
  }

  case class ExecutionDetails(jobExecutions: Seq[ExecutionLog], parentExecutions: Seq[ExecutionLog])

  private implicit val executionDetailsEncoder: Encoder[ExecutionDetails] =
    Encoder.forProduct2("jobExecutions", "parentExecutions")(
      e => (e.jobExecutions, e.parentExecutions)
    )

  private[cuttle] override def publicRoutes(workflow: Workflow[TimeSeries],
                                            executor: Executor[TimeSeries],
                                            xa: XA): PartialService = {

    case request @ GET at url"/api/timeseries/executions?job=$jobId&start=$start&end=$end" =>
      type WatchedState = ((State, Set[Backfill]), Set[(Job[TimeSeries], TimeSeriesContext)])

      def watchState(): Option[WatchedState] = Some((state, executor.allFailingJobsWithContext))

      def getExecutions: IO[Json] = {
        val job = workflow.vertices.find(_.id == jobId).get
        val calendar = job.scheduling.calendar
        val startDate = Instant.parse(start)
        val endDate = Instant.parse(end)
        val requestedInterval = Interval(startDate, endDate)
        val contextQuery = Database.sqlGetContextsBetween(Some(startDate), Some(endDate))
        val archivedExecutions =
          executor.archivedExecutions(contextQuery, Set(jobId), "", asc = true, 0, Int.MaxValue, xa)
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
              .intersect(requestedInterval)
              .toList
              .collect { case (itvl, Todo(maybeBackfill)) => (itvl, maybeBackfill) }
            (lo, hi) <- calendar.split(interval)
          } yield {
            val context = TimeSeriesContext(calendar.truncate(lo), calendar.ceil(hi), maybeBackfill)
            ExecutionLog("", job.id, None, None, context.asJson, ExecutionTodo, None, 0)
          }
        val throttledExecutions = executor.allFailingExecutions
          .filter(e => e.job == job && e.context.toInterval.intersects(requestedInterval))
          .map(_.toExecutionLog(ExecutionThrottled))

        archivedExecutions.map(
          archivedExecutions =>
            ExecutionDetails(archivedExecutions ++ runningExecutions ++ remainingExecutions ++ throttledExecutions,
                             parentExecutions(requestedInterval, job, state)).asJson)
      }

      def parentExecutions(requestedInterval: Interval[Instant],
                           job: Job[TimeSeries],
                           state: Map[Job[TimeSeries], IntervalMap[Instant, JobState]]): Seq[ExecutionLog] = {

        val calendar = job.scheduling.calendar
        val parentJobs = workflow.edges
          .collect({ case (child, parent, _) if child == job => parent })
        val runningDependencies: Seq[ExecutionLog] = executor.runningExecutions
          .filter {
            case (e, _) => parentJobs.contains(e.job) && e.context.toInterval.intersects(requestedInterval)
          }
          .map({ case (e, status) => e.toExecutionLog(status) })
        val failingDependencies: Seq[ExecutionLog] = executor.allFailingExecutions
          .filter(e => parentJobs.contains(e.job) && e.context.toInterval.intersects(requestedInterval))
          .map(_.toExecutionLog(ExecutionThrottled))
        val remainingDependenciesDeps =
          for {
            parentJob <- parentJobs
            (interval, maybeBackfill) <- state(parentJob)
              .intersect(requestedInterval)
              .toList
              .collect { case (itvl, Todo(maybeBackfill)) => (itvl, maybeBackfill) }
            (lo, hi) <- calendar.split(interval)
          } yield {
            val context = TimeSeriesContext(calendar.truncate(lo), calendar.ceil(hi), maybeBackfill)
            ExecutionLog("", parentJob.id, None, None, context.asJson, ExecutionTodo, None, 0)
          }

        runningDependencies ++ failingDependencies ++ remainingDependenciesDeps.toSeq
      }

      if (request.headers.get(h"Accept").contains(h"text/event-stream"))
        sse(IO { watchState() }, (s: WatchedState) => getExecutions)
      else
        getExecutions.map(Ok(_))

    case request @ GET at url"/api/timeseries/calendar/focus?start=$start&end=$end&jobs=$jobs" =>
      type WatchedState = ((State, Set[Backfill]), Set[(Job[TimeSeries], TimeSeriesContext)])

      val filteredJobs = Try(jobs.split(",").toSeq.filter(_.nonEmpty)).toOption
        .filter(_.nonEmpty)
        .getOrElse(workflow.vertices.map(_.id))
        .toSet

      def watchState: Option[WatchedState] = Some((state, executor.allFailingJobsWithContext))

      def getFocusView = {
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

        val pausedJobs = executor.pausedJobs
        val allFailing = executor.allFailingExecutions
        val allWaitingIds = executor.allRunning
          .filter(_.status == ExecutionWaiting)
          .map(_.id)

        def findAggregationLevel(n: Int,
                                 calendarView: TimeSeriesCalendarView,
                                 interval: Interval[Instant]): TimeSeriesCalendarView = {
          val aggregatedExecutions = calendarView.calendar.split(interval)
          if (aggregatedExecutions.size <= n)
            calendarView
          else
            findAggregationLevel(n, calendarView.upper(), interval)
        }

        def aggregateExecutions(
          job: TimeSeriesJob,
          period: Interval[Instant],
          calendarView: TimeSeriesCalendarView): List[(Interval[Instant], List[(Interval[Instant], JobState)])] =
          calendarView.calendar
            .split(period)
            .flatMap { interval =>
              {
                val (start, end) = interval
                val currentlyAggregatedPeriod = state(job)
                  .intersect(Interval(start, end))
                  .toList
                  .sortBy(_._1.lo)
                currentlyAggregatedPeriod match {
                  case Nil => None
                  case _   => Some((Interval(start, end), currentlyAggregatedPeriod))
                }
              }
            }

        def getStatusLabelFromState(jobState: JobState, job: Job[TimeSeries]): String =
          jobState match {
            case Running(e) =>
              if (allFailing.exists(_.id == e))
                "failed"
              else if (allWaitingIds.contains(e))
                "waiting"
              else if (pausedJobs.contains(job.id))
                "paused"
              else "running"
            case Todo(_) => "todo"
            case Done    => "successful"
          }

        val jobTimelines =
          (for { job <- workflow.vertices if filteredJobs.contains(job.id) } yield {
            val calendarView = findAggregationLevel(
              48,
              TimeSeriesCalendarView(job.scheduling.calendar),
              period
            )
            val jobExecutions = for {
              (interval, jobStatesOnIntervals) <- aggregateExecutions(job, period, calendarView)
            } yield {
              val inBackfill = backfills.exists(
                bf =>
                  bf.jobs.contains(job) &&
                    IntervalMap(interval -> 0)
                      .intersect(Interval(bf.start, bf.end))
                      .toList
                      .nonEmpty)
              if (calendarView.aggregationFactor == 1)
                jobStatesOnIntervals match {
                  case (_, state) :: Nil =>
                    Some(JobExecution(interval, getStatusLabelFromState(state, job), inBackfill))
                  case _ => None
                } else
                jobStatesOnIntervals match {
                  case l if l.nonEmpty => {
                    val (duration, done, error) = l.foldLeft((0L, 0L, false))((acc, currentExecution) => {
                      val (lo, hi) = currentExecution._1.toPair
                      val jobStatus = getStatusLabelFromState(currentExecution._2, job)
                      (acc._1 + lo.until(hi, SECONDS),
                       acc._2 + (if (jobStatus == "successful") lo.until(hi, SECONDS) else 0),
                       acc._3 || jobStatus == "failed")
                    })
                    Some(
                      AggregatedJobExecution(interval, f"${done.toDouble / duration.toDouble}%2.2f", error, inBackfill))
                  }
                  case Nil => None
                }
            }
            JobTimeline(job.id, calendarView, jobExecutions.flatten)
          }).toList

        Json.obj(
          "summary" -> jobTimelines
            .maxBy(_.executions.size)
            .calendarView
            .calendar
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
                    case _          => false
                  })
                }
                if (jobSummaries.nonEmpty) {
                  val (duration, done, failing) = jobSummaries
                    .reduce((a, b) => (a._1 + b._1, a._2 + b._2, a._3 || b._3))
                  Some(
                    AggregatedJobExecution(Interval(lo, hi),
                                           f"${done.toDouble / duration.toDouble}%2.2f",
                                           failing,
                                           isInbackfill)
                  )
                } else {
                  None
                }
            }
            .asJson,
          "jobs" -> jobTimelines.map(jt => jt.jobId -> jt).toMap.asJson
        )
      }

      if (request.headers.get(h"Accept").contains(h"text/event-stream"))
        sse(IO { watchState }, (_: WatchedState) => IO(getFocusView))
      else
        Ok(getFocusView)

    case request @ GET at url"/api/timeseries/calendar?jobs=$jobs" =>
      val filteredJobs = Try(jobs.split(",").toSeq.filter(_.nonEmpty)).toOption
        .filter(_.nonEmpty)
        .getOrElse(workflow.vertices.map(_.id))
        .toSet

      type WatchedState = ((TimeSeriesUtils.State, Set[Backfill]), Set[(Job[TimeSeries], TimeSeries#Context)])
      def watchState: Option[WatchedState] = Some((state, executor.allFailingJobsWithContext))

      def getCalendar(): Json = {
        val (state, backfills) = this.state
        val backfillDomain =
          backfills.foldLeft(IntervalMap.empty[Instant, Unit]) { (acc, bf) =>
            if (bf.jobs.map(_.id).intersect(filteredJobs).nonEmpty)
              acc.update(Interval(bf.start, bf.end), ())
            else
              acc
          }
        val upToMidnightToday = Interval(Bottom, Finite(Daily(UTC).ceil(Instant.now)))
        (for {
          job <- workflow.vertices.toList
          if filteredJobs.contains(job.id)
          (interval, jobState) <- state(job).intersect(upToMidnightToday).toList
          (start, end) <- Daily(UTC).split(interval)
        } yield
          (Daily(UTC).truncate(start), start.until(end, SECONDS), jobState == Done, jobState match {
            case Running(exec) => executor.allFailingExecutions.exists(_.id == exec)
            case _             => false
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
              val completion = Math.floor((done.toDouble / total.toDouble) * 10) / 10
              val correctedCompletion =
                if (completion == 0 && done != 0) 0.1
                else completion
              Map(
                "date" -> date.asJson,
                "completion" -> f"$correctedCompletion%.1f".asJson
              ) ++ (if (stuck) Map("stuck" -> true.asJson) else Map.empty) ++
                (if (backfillDomain.intersect(Interval(date, Daily(UTC).next(date))).toList.nonEmpty)
                   Map("backfill" -> true.asJson)
                 else Map.empty)
          }
          .asJson

      }

      if (request.headers.get(h"Accept").contains(h"text/event-stream"))
        sse(IO { watchState }, (_: WatchedState) => IO.pure(getCalendar()))
      else
        Ok(getCalendar())

    case GET at url"/api/timeseries/backfills" =>
      Database
        .queryBackfills()
        .list
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
        .map(backfills => Ok(backfills.asJson))
    case GET at url"/api/timeseries/backfills/$id?events=$events" =>
      val backfills = Database.getBackfillById(id).transact(xa)
      events match {
        case "true" | "yes" => sse(backfills, (b: Json) => IO.pure(b))
        case _              => backfills.map(bf => Ok(bf.asJson))
      }
    case GET at url"/api/timeseries/backfills/$backfillId/executions?events=$events&limit=$l&offset=$o&sort=$sort&order=$a" => {
      val limit = Try(l.toInt).toOption.getOrElse(25)
      val offset = Try(o.toInt).toOption.getOrElse(0)
      val asc = (a.toLowerCase == "asc")
      def asTotalJson(x: (Int, Double, Seq[ExecutionLog])) = x match {
        case (total, completion, executions) =>
          Json.obj(
            "total" -> total.asJson,
            "offset" -> offset.asJson,
            "limit" -> limit.asJson,
            "sort" -> sort.asJson,
            "asc" -> asc.asJson,
            "data" -> executions.asJson,
            "completion" -> completion.asJson
          )
      }
      val ordering = {
        val columnOrdering = sort match {
          case "job"       => Ordering.by((_: ExecutionLog).job)
          case "startTime" => Ordering.by((_: ExecutionLog).startTime)
          case "status"    => Ordering.by((_: ExecutionLog).status.toString)
          case _           => Ordering.by((_: ExecutionLog).id)
        }
        if (asc) {
          columnOrdering
        } else {
          columnOrdering.reverse
        }
      }
      def allExecutions(): IO[Option[(Int, Double, List[ExecutionLog])]] = {

        val runningExecutions = executor.runningExecutions
          .filter(t => {
            val bf = t._1.context.backfill
            bf.isDefined && bf.get.id == backfillId
          })
          .map({ case (execution, status) => execution.toExecutionLog(status) })

        val runningExecutionsIds = runningExecutions.map(_.id).toSet
        Database
          .getExecutionLogsForBackfill(backfillId)
          .transact(xa)
          .map(archived => {
            val archivedNotRunning = archived.filterNot(e => runningExecutionsIds.contains(e.id))
            val executions = runningExecutions ++ archivedNotRunning
            val completion = {
              executions.size match {
                case 0     => 0
                case total => (total - runningExecutions.size).toDouble / total
              }
            }
            Some((executions.size, completion, executions.sorted(ordering).drop(offset).take(limit).toList))
          })
      }

      events match {
        case "true" | "yes" =>
          sse(allExecutions(), (e: (Int, Double, List[ExecutionLog])) => IO.pure(asTotalJson(e)))
        case _ =>
          allExecutions().map(_.map(e => Ok(asTotalJson(e))).getOrElse(NotFound))
      }
    }
  }

  private[cuttle] override def privateRoutes(workflow: Workflow[TimeSeries],
                                             executor: Executor[TimeSeries],
                                             xa: XA): AuthenticatedService = {

    case req @ POST at url"/api/timeseries/backfill" =>
      implicit user =>
        req
          .readAs[Json]
          .flatMap(
            _.as[BackfillCreate]
              .fold(
                _ => IO.pure(BadRequest("cannot parse request body")),
                backfill => {
                  val jobIds = backfill.jobs.split(",")
                  val jobs = workflow.vertices
                    .filter((job: TimeSeriesJob) => jobIds.contains(job.id))

                  backfillJob(backfill.name,
                              backfill.description,
                              jobs,
                              backfill.startDate,
                              backfill.endDate,
                              backfill.priority,
                              xa).map({
                    case true => Ok("ok".asJson)
                    case _    => BadRequest("invalid backfill")
                  })
                }
              )
          )
  }
}
