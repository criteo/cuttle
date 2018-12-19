package com.criteo.cuttle.timeseries

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit

import scala.concurrent.duration._
import scala.util._

import cats.Eq
import cats.effect.IO
import cats.implicits._
import doobie.implicits._
import fs2.Stream
import io.circe._
import io.circe.generic.auto._
import io.circe.java8.time._
import io.circe.syntax._
import lol.http._
import lol.json._

import com.criteo.cuttle.Auth._
import com.criteo.cuttle.ExecutionStatus._
import com.criteo.cuttle.Metrics.{Gauge, Prometheus}
import com.criteo.cuttle._
import com.criteo.cuttle.utils.getJVMUptime
import com.criteo.cuttle.events.JobSuccessForced
import com.criteo.cuttle.timeseries.TimeSeriesUtils._
import com.criteo.cuttle.timeseries.intervals.Bound.{Bottom, Finite, Top}
import com.criteo.cuttle.timeseries.intervals._

private[timeseries] object TimeSeriesApp {

  def sse[A](thunk: IO[Option[A]], encode: A => IO[Json])(implicit eqInstance: Eq[A]): lol.http.Response = {
    import com.criteo.cuttle.ThreadPools.Implicits.serverContextShift
    import com.criteo.cuttle.utils.timer

    val stream = (Stream.emit(()) ++ Stream.fixedRate[IO](1.seconds))
      .evalMap[IO, Option[A]](_ => IO.shift.flatMap(_ => thunk))
      .flatMap({
        case Some(x) => Stream(x)
        case None    => Stream.raiseError[IO](new RuntimeException("Could not get result to stream"))
      })
      .changes
      .evalMap[IO, Json](r => encode(r))
      .map(ServerSentEvents.Event(_))

    Ok(stream)
  }

  implicit def projectEncoder = new Encoder[CuttleProject] {
    override def apply(project: CuttleProject) =
      Json.obj(
        "name" -> project.name.asJson,
        "version" -> Option(project.version).filterNot(_.isEmpty).asJson,
        "description" -> Option(project.description).filterNot(_.isEmpty).asJson,
        "scheduler" -> project.scheduler.name.asJson,
        "env" -> Json.obj(
          "name" -> Option(project.env._1).filterNot(_.isEmpty).asJson,
          "critical" -> project.env._2.asJson
        )
      )
  }

  implicit val executionStatEncoder: Encoder[ExecutionStat] = new Encoder[ExecutionStat] {
    override def apply(execution: ExecutionStat): Json =
      Json.obj(
        "startTime" -> execution.startTime.asJson,
        "endTime" -> execution.endTime.asJson,
        "durationSeconds" -> execution.durationSeconds.asJson,
        "waitingSeconds" -> execution.waitingSeconds.asJson,
        "status" -> (execution.status match {
          case ExecutionSuccessful => "successful"
          case ExecutionFailed     => "failed"
          case ExecutionRunning    => "running"
          case ExecutionWaiting    => "waiting"
          case ExecutionPaused     => "paused"
          case ExecutionThrottled  => "throttled"
          case ExecutionTodo       => "todo"
        }).asJson
      )
  }

  implicit val tagEncoder = new Encoder[Tag] {
    override def apply(tag: Tag) =
      Json.obj(
        "name" -> tag.name.asJson,
        "description" -> Option(tag.description).filterNot(_.isEmpty).asJson
      )
  }

  implicit def jobEncoder = new Encoder[Job[TimeSeries]] {
    override def apply(job: Job[TimeSeries]) =
      Json
        .obj(
          "id" -> job.id.asJson,
          "name" -> Option(job.name).filterNot(_.isEmpty).getOrElse(job.id).asJson,
          "description" -> Option(job.description).filterNot(_.isEmpty).asJson,
          "scheduling" -> job.scheduling.asJson,
          "tags" -> job.tags.map(_.name).asJson
        )
        .asJson
  }

  import io.circe.generic.semiauto._
  implicit val encodeUser: Encoder[User] = deriveEncoder
  implicit val encodePausedJob: Encoder[PausedJob] = deriveEncoder
}

private[timeseries] case class TimeSeriesApp(project: CuttleProject, executor: Executor[TimeSeries], xa: XA) {

  import project.{jobs, scheduler}

  import JobState._
  import TimeSeriesApp._
  import TimeSeriesCalendar._

  private val allIds = jobs.all.map(_.id)

  private def parseJobIds(jobsQueryString: String): Set[String] =
    jobsQueryString.split(",").filter(_.trim().nonEmpty).toSet

  private def getJobsOrNotFound(jobsQueryString: String): Either[Response, Set[Job[TimeSeries]]] = {
    val jobsNames = parseJobIds(jobsQueryString)
    if (jobsNames.isEmpty) Right(jobs.all)
    else {
      val filteredJobs = jobs.all.filter(v => jobsNames.contains(v.id))
      if (filteredJobs.isEmpty) Left(NotFound)
      else Right(filteredJobs)
    }
  }

  val publicApi: PartialService = {

    case GET at url"/api/status" => {
      val projectJson = (status: String) =>
        Json.obj(
          "project" -> project.name.asJson,
          "version" -> Option(project.version).filterNot(_.isEmpty).asJson,
          "status" -> status.asJson
      )
      executor.healthCheck() match {
        case Success(_) => Ok(projectJson("ok"))
        case _          => InternalServerError(projectJson("ko"))
      }
    }

    case GET at url"/api/statistics?events=$events&jobs=$jobs" =>
      val jobIds = parseJobIds(jobs)
      val ids = if (jobIds.isEmpty) allIds else jobIds

      def getStats: IO[Option[(Json, Json)]] =
        executor
          .getStats(ids)
          .map(
            stats =>
              Try(
                stats -> scheduler.getStats(ids)
              ).toOption)

      def asJson(x: (Json, Json)) = x match {
        case (executorStats, schedulerStats) =>
          executorStats.deepMerge(
            Json.obj(
              "scheduler" -> schedulerStats
            ))
      }

      events match {
        case "true" | "yes" =>
          sse(IO.suspend(getStats), (x: (Json, Json)) => IO(asJson(x)))
        case _ => getStats.map(_.map(stat => Ok(asJson(stat))).getOrElse(InternalServerError))
      }

    case GET at url"/api/statistics/$jobName" =>
      executor
        .jobStatsForLastThirtyDays(jobName)
        .map(stats => Ok(stats.asJson))

    case GET at url"/version" => Ok(project.version)

    case GET at "/metrics" =>
      val metrics =
        executor.getMetrics(allIds, jobs) ++
          scheduler.getMetrics(allIds, jobs) :+
          Gauge("cuttle_jvm_uptime_seconds").labeled(("version", project.version), getJVMUptime)
      Ok(Prometheus.serialize(metrics))

    case GET at url"/api/executions/status/$kind?limit=$l&offset=$o&events=$events&sort=$sort&order=$order&jobs=$jobs" =>
      logger.debug(s"Retreiving $kind executions with sse mode $events")
      val jobIds = parseJobIds(jobs)
      val limit = Try(l.toInt).toOption.getOrElse(25)
      val offset = Try(o.toInt).toOption.getOrElse(0)
      val asc = order.toLowerCase == "asc"
      val ids = if (jobIds.isEmpty) allIds else jobIds

      def getExecutions: IO[Option[(Int, List[ExecutionLog])]] = kind match {
        case "started" =>
          IO(
            Some(
              executor.runningExecutionsSizeTotal(ids) -> executor
                .runningExecutions(ids, sort, asc, offset, limit)
                .toList))
        case "stuck" =>
          IO(
            Some(
              executor.failingExecutionsSize(ids) -> executor
                .failingExecutions(ids, sort, asc, offset, limit)
                .toList))
        case "finished" =>
          executor
            .archivedExecutionsSize(ids)
            .map(ids => Some(ids -> executor.allRunning.toList))
        case _ =>
          IO.pure(None)
      }

      def asJson(x: (Int, Seq[ExecutionLog])): IO[Json] = x match {
        case (total, executions) =>
          (kind match {
            case "finished" =>
              executor
                .archivedExecutions(scheduler.allContexts, ids, sort, asc, offset, limit, xa)
                .map(execs => execs.asJson)
            case _ =>
              IO(executions.asJson)
          }).map(
            data =>
              Json.obj(
                "total" -> total.asJson,
                "offset" -> offset.asJson,
                "limit" -> limit.asJson,
                "sort" -> sort.asJson,
                "asc" -> asc.asJson,
                "data" -> data
            ))
      }

      events match {
        case "true" | "yes" =>
          sse(getExecutions, asJson)
        case _ =>
          getExecutions
            .flatMap(
              _.map(e => asJson(e).map(json => Ok(json)))
                .getOrElse(NotFound))
      }

    case GET at url"/api/executions/$id?events=$events" =>
      def getExecution = IO.suspend(executor.getExecution(scheduler.allContexts, id))

      events match {
        case "true" | "yes" =>
          sse(getExecution, (e: ExecutionLog) => IO(e.asJson))
        case _ =>
          getExecution.map(_.map(e => Ok(e.asJson)).getOrElse(NotFound))
      }

    case req @ GET at url"/api/executions/$id/streams" =>
      lazy val streams = executor.openStreams(id)
      req.headers.get(h"Accept").contains(h"text/event-stream") match {
        case true =>
          Ok(
            fs2.Stream(ServerSentEvents.Event("BOS".asJson)) ++
              streams
                .through(fs2.text.utf8Decode)
                .through(fs2.text.lines)
                .chunks
                .map(chunk => ServerSentEvents.Event(Json.fromValues(chunk.toArray.toIterable.map(_.asJson)))) ++
              fs2.Stream(ServerSentEvents.Event("EOS".asJson))
          )
        case false =>
          Ok(
            Content(
              stream = streams,
              headers = Map(h"Content-Type" -> h"text/plain")
            ))
      }

    case GET at "/api/jobs/paused" =>
      Ok(scheduler.pausedJobs().asJson)

    case GET at "/api/project_definition" =>
      Ok(project.asJson)

    case GET at "/api/jobs_definition" =>
      Ok(jobs.asJson)
  }

  val privateApi: AuthenticatedService = {
    case POST at url"/api/executions/$id/cancel" => { implicit user =>
      executor.cancelExecution(id)
      Ok
    }

    case POST at url"/api/jobs/pause?jobs=$jobs" => { implicit user =>
      getJobsOrNotFound(jobs).fold(IO.pure, jobs => {
        scheduler.pauseJobs(jobs, executor, xa)
        Ok
      })
    }

    case POST at url"/api/jobs/resume?jobs=$jobs" => { implicit user =>
      getJobsOrNotFound(jobs).fold(IO.pure, jobs => {
        scheduler.resumeJobs(jobs, xa)
        Ok
      })
    }
    case POST at url"/api/jobs/all/unpause" => { implicit user =>
      scheduler.resumeJobs(jobs.all, xa)
      Ok
    }
    case POST at url"/api/jobs/$id/unpause" => { implicit user =>
      jobs.all.find(_.id == id).fold(NotFound) { job =>
        scheduler.resumeJobs(Set(job), xa)
        Ok
      }
    }
    case POST at url"/api/executions/relaunch?jobs=$jobs" => { implicit user =>
      val filteredJobs = Try(jobs.split(",").toSeq.filter(_.nonEmpty)).toOption
        .filter(_.nonEmpty)
        .getOrElse(allIds)
        .toSet

      executor.relaunch(filteredJobs)
      IO.pure(Ok)
    }

    case req @ GET at url"/api/shutdown" => { implicit user =>
      import scala.concurrent.duration._

      req.queryStringParameters.get("gracePeriodSeconds") match {
        case Some(s) =>
          Try(s.toLong) match {
            case Success(s) if s > 0 =>
              executor.gracefulShutdown(Duration(s, TimeUnit.SECONDS))
              Ok
            case _ =>
              BadRequest("gracePeriodSeconds should be a positive integer")
          }
        case None =>
          req.queryStringParameters.get("hard") match {
            case Some(_) =>
              executor.hardShutdown()
              Ok
            case None =>
              BadRequest("Either gracePeriodSeconds or hard should be specified as query parameter")
          }
      }
    }
  }

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
  private val queries = new Queries {}

  private trait ExecutionPeriod {
    val period: Interval[Instant]
    val backfill: Boolean
    val aggregated: Boolean
    val version: String
  }

  private case class JobExecution(period: Interval[Instant], status: String, backfill: Boolean, version: String)
      extends ExecutionPeriod {
    override val aggregated: Boolean = false
  }

  private case class AggregatedJobExecution(period: Interval[Instant],
                                            completion: Double,
                                            error: Boolean,
                                            backfill: Boolean,
                                            version: String = "")
      extends ExecutionPeriod {
    override val aggregated: Boolean = true
  }

  private case class JobTimeline(jobId: String, calendarView: TimeSeriesCalendarView, executions: List[ExecutionPeriod])

  private implicit val executionPeriodEncoder = new Encoder[ExecutionPeriod] {
    override def apply(executionPeriod: ExecutionPeriod) = {
      val coreFields = List(
        "period" -> executionPeriod.period.asJson,
        "backfill" -> executionPeriod.backfill.asJson,
        "aggregated" -> executionPeriod.aggregated.asJson,
        "version" -> executionPeriod.version.asJson
      )
      val finalFields = executionPeriod match {
        case JobExecution(_, status, _, _) => ("status" -> status.asJson) :: coreFields
        case AggregatedJobExecution(_, completion, error, _, _) =>
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

  private[timeseries] def publicRoutes(): PartialService = {
    case request @ GET at url"/api/timeseries/executions?job=$jobId&start=$start&end=$end" =>
      type WatchedState = ((State, Set[Backfill]), Set[(Job[TimeSeries], TimeSeriesContext)])

      def watchState(): Option[WatchedState] = Some((scheduler.state, executor.allFailingJobsWithContext))

      def getExecutions(watchedState: WatchedState): IO[Json] = {
        val job = jobs.vertices.find(_.id == jobId).get
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

        val ((jobStates, _), _) = watchedState
        val remainingExecutions =
          for {
            (interval, maybeBackfill) <- jobStates(job)
              .intersect(requestedInterval)
              .toList
              .collect { case (itvl, Todo(maybeBackfill)) => (itvl, maybeBackfill) }
            (lo, hi) <- calendar.split(interval)
          } yield {
            val context =
              TimeSeriesContext(calendar.truncate(lo), calendar.ceil(hi), maybeBackfill, executor.projectVersion)
            ExecutionLog("", job.id, None, None, context.asJson, ExecutionTodo, None, 0)
          }
        val throttledExecutions = executor.allFailingExecutions
          .filter(e => e.job == job && e.context.toInterval.intersects(requestedInterval))
          .map(_.toExecutionLog(ExecutionThrottled))

        archivedExecutions.map(
          archivedExecutions =>
            ExecutionDetails(archivedExecutions ++ runningExecutions ++ remainingExecutions ++ throttledExecutions,
                             parentExecutions(requestedInterval, job, jobStates)).asJson)
      }

      def parentExecutions(requestedInterval: Interval[Instant],
                           job: Job[TimeSeries],
                           state: Map[Job[TimeSeries], IntervalMap[Instant, JobState]]): Seq[ExecutionLog] = {

        val calendar = job.scheduling.calendar
        val parentJobs = jobs.edges
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
            val context =
              TimeSeriesContext(calendar.truncate(lo), calendar.ceil(hi), maybeBackfill, executor.projectVersion)
            ExecutionLog("", parentJob.id, None, None, context.asJson, ExecutionTodo, None, 0)
          }

        runningDependencies ++ failingDependencies ++ remainingDependenciesDeps.toSeq
      }

      watchState() match {
        case Some(watchedState) =>
          if (request.headers.get(h"Accept").contains(h"text/event-stream"))
            sse(IO { Some(watchedState) }, (s: WatchedState) => getExecutions(s))
          else
            getExecutions(watchedState).map(Ok(_))
        case None =>
          BadRequest
      }

    case request @ GET at url"/api/timeseries/calendar/focus?start=$start&end=$end&jobs=$jobs" =>
      type WatchedState = ((State, Set[Backfill]), Set[(Job[TimeSeries], TimeSeriesContext)])

      val filteredJobs = Try(jobs.split(",").toSeq.filter(_.nonEmpty)).toOption
        .filter(_.nonEmpty)
        .getOrElse(project.jobs.all.map(_.id))
        .toSet

      def watchState(): Option[WatchedState] = Some((scheduler.state, executor.allFailingJobsWithContext))

      def getFocusView(watchedState: WatchedState) = {
        val startDate = Instant.parse(start)
        val endDate = Instant.parse(end)
        val period = Interval(startDate, endDate)
        val ((jobStates, backfills), _) = watchedState
        val backfillDomain =
          backfills.foldLeft(IntervalMap.empty[Instant, Unit]) { (acc, bf) =>
            if (bf.jobs.map(_.id).intersect(filteredJobs).nonEmpty)
              acc.update(Interval(bf.start, bf.end), ())
            else
              acc
          }

        val pausedJobs = scheduler.pausedJobs().map(_.id)
        val allFailingExecutionIds = executor.allFailingExecutions.map(_.id).toSet
        val allWaitingExecutionIds = executor.allRunning
          .filter(_.status == ExecutionWaiting)
          .map(_.id)
          .toSet

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
                val currentlyAggregatedPeriod = jobStates(job)
                  .intersect(Interval(start, end))
                  .toList
                  .sortBy(_._1.lo)
                currentlyAggregatedPeriod match {
                  case Nil => None
                  case _   => Some((Interval(start, end), currentlyAggregatedPeriod))
                }
              }
            }

        def getVersionFromState(jobState: JobState): String = jobState match {
          case Done(version) => version
          case _             => ""
        }

        def getStatusLabelFromState(jobState: JobState, job: Job[TimeSeries]): String =
          jobState match {
            case Running(executionId) =>
              if (allFailingExecutionIds.contains(executionId))
                "failed"
              else if (allWaitingExecutionIds.contains(executionId))
                "waiting"
              else if (pausedJobs.contains(job.id))
                "paused"
              else "running"
            case Todo(_) => if (pausedJobs.contains(job.id)) "paused" else "todo"
            case Done(_) => "successful"
          }

        val jobTimelines =
          (for { job <- project.jobs.all if filteredJobs.contains(job.id) } yield {
            val calendarView = findAggregationLevel(
              48,
              TimeSeriesCalendarView(job.scheduling.calendar),
              period
            )
            val jobExecutions: List[Option[ExecutionPeriod]] = for {
              (interval, jobStatesOnIntervals) <- aggregateExecutions(job, period, calendarView)
            } yield {
              val inBackfill = backfills.exists(
                bf =>
                  bf.jobs.contains(job) &&
                    IntervalMap(interval -> 0)
                      .intersect(Interval(bf.start, bf.end))
                      .toList
                      .nonEmpty)
              if (calendarView.aggregationFactor == 1) {
                jobStatesOnIntervals match {
                  case (_, state) :: Nil =>
                    Some(JobExecution(interval, getStatusLabelFromState(state, job), inBackfill, getVersionFromState(state)))
                  case _ => None
                }
              }
              else {
                jobStatesOnIntervals match {
                  case jobStates: List[(Interval[Instant], JobState)] if jobStates.nonEmpty => {
                    val (duration, done, error) = jobStates.foldLeft((0L, 0L, false)) {
                      case ((accumulatedDuration, accumulatedDoneDuration, hasErrors), (period, jobState)) =>
                        val (lo, hi) = period.toPair
                        val jobStatus = getStatusLabelFromState(jobState, job)
                        (accumulatedDuration + lo.until(hi, ChronoUnit.SECONDS),
                         accumulatedDoneDuration + (if (jobStatus == "successful") lo.until(hi, ChronoUnit.SECONDS) else 0),
                         hasErrors || jobStatus == "failed")
                    }
                    Some(AggregatedJobExecution(interval, done.toDouble / duration.toDouble, error, inBackfill))
                  }
                  case Nil => None
                }
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

              case class JobSummary(periodLengthInSeconds: Long, periodDoneInSeconds: Long, hasErrors: Boolean)

                val jobSummaries: Set[JobSummary] = for {
                  job <- project.jobs.all
                  if filteredJobs.contains(job.id)
                  (interval, jobState) <- jobStates(job).intersect(Interval(lo, hi)).toList
                } yield {
                  val (lo, hi) = interval.toPair
                  JobSummary(
                    periodLengthInSeconds = lo.until(hi, ChronoUnit.SECONDS),
                    periodDoneInSeconds = jobState match {
                      case Done(_) => lo.until(hi, ChronoUnit.SECONDS)
                      case _       => 0
                    },
                    hasErrors = jobState match {
                      case Running(executionId) => allFailingExecutionIds.contains(executionId)
                      case _          => false
                    })
                }
                if (jobSummaries.nonEmpty) {
                  val aggregatedJobSummary = jobSummaries.reduce { (a: JobSummary, b: JobSummary) =>
                    JobSummary(a.periodLengthInSeconds + b.periodLengthInSeconds,
                               a.periodDoneInSeconds + b.periodDoneInSeconds,
                               a.hasErrors || b.hasErrors)
                  }
                  Some(
                    AggregatedJobExecution(
                      Interval(lo, hi),
                      aggregatedJobSummary.periodDoneInSeconds.toDouble / aggregatedJobSummary.periodLengthInSeconds.toDouble,
                      aggregatedJobSummary.hasErrors, isInbackfill
                    )
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
        sse(IO { watchState() }, (s: WatchedState) => IO(getFocusView(s)))
      else
        watchState() match {
          case Some(watchedState) => Ok(getFocusView(watchedState))
          case None               => BadRequest
        }

    case request @ GET at url"/api/timeseries/calendar?jobs=$jobs" =>
      val filteredJobs = Try(jobs.split(",").toSeq.filter(_.nonEmpty)).toOption
        .filter(_.nonEmpty)
        .getOrElse(project.jobs.all.map(_.id))
        .toSet

      type WatchedState = ((TimeSeriesUtils.State, Set[Backfill]), Set[(Job[TimeSeries], TimeSeries#Context)])

      case class JobStateOnPeriod(start: Instant, duration: Long, isDone: Boolean, isStuck: Boolean)

      def watchState(): Option[WatchedState] = Some((scheduler.state, executor.allFailingJobsWithContext))

      def getCalendar(watchedState: WatchedState): Json = {
        val ((jobStates, backfills), _) = watchedState
        val backfillDomain =
          backfills.foldLeft(IntervalMap.empty[Instant, Unit]) { (acc, bf) =>
            if (bf.jobs.map(_.id).intersect(filteredJobs).nonEmpty)
              acc.update(Interval(bf.start, bf.end), ())
            else
              acc
          }
        val upToMidnightToday = Interval(Bottom, Finite(Daily(UTC).ceil(Instant.now)))

        lazy val failingExecutionIds = executor.allFailingExecutions.map(_.id).toSet
        val jobStatesOnPeriod: Set[JobStateOnPeriod] = for {
          job <- project.jobs.all
          if filteredJobs.contains(job.id)
          (interval, jobState) <- jobStates(job).intersect(upToMidnightToday).toList
          (start, end) <- Daily(UTC).split(interval)
        } yield
          JobStateOnPeriod(
            Daily(UTC).truncate(start),
            start.until(end, ChronoUnit.SECONDS),
            jobState match {
              case Done(_) => true
              case _       => false
            },
            jobState match {
              case Running(exec) => failingExecutionIds.contains(exec)
              case _             => false
            }
          )

        jobStatesOnPeriod
          .groupBy { case JobStateOnPeriod(start, _, _, _) => start }
          .toList
          .sortBy { case (periodStart, _) => periodStart }
          .map {
            case (date, statesOnPeriod) =>
              val (total, done, stuck) = statesOnPeriod.foldLeft((0L, 0L, false)) {
                case (acc, JobStateOnPeriod(_, duration, isDone, isStuck)) =>
                  val (totalDuration, doneDuration, isAnyStuck) = acc
                  val newDone = if (isDone) duration else 0L
                  (totalDuration + duration, doneDuration + newDone, isAnyStuck || isStuck)
              }
              val completion = Math.floor((done.toDouble / total.toDouble) * 10) / 10
              val correctedCompletion =
                if (completion == 0 && done != 0) 0.1
                else completion
              Map(
                "date" -> date.atZone(UTC).toLocalDate.asJson,
                "completion" -> correctedCompletion.asJson
              ) ++ (if (stuck) Map("stuck" -> true.asJson) else Map.empty) ++
                (if (backfillDomain.intersect(Interval(date, Daily(UTC).next(date))).toList.nonEmpty)
                   Map("backfill" -> true.asJson)
                 else Map.empty)
          }
          .asJson

      }

      if (request.headers.get(h"Accept").contains(h"text/event-stream"))
        sse(IO { watchState() }, (s: WatchedState) => IO.pure(getCalendar(s)))
      else
        watchState() match {
          case Some(watchedState) => Ok(getCalendar(watchedState))
          case None               => BadRequest
        }

    case GET at url"/api/timeseries/lastruns?job=$jobId" =>
      val (jobStates, _) = scheduler.state
      val successfulIntervalMaps = jobStates
        .filter(s => s._1.id == jobId)
        .values
        .flatMap(m => m.toList)
        .filter {
          case (interval, jobState) =>
            jobState match {
              case Done(_) => true
              case _       => false
            }
        }
        .foldLeft(IntervalMap.empty[Instant, Unit])((acc, elt) => acc.update(elt._1, ()))
        .toList

      if (successfulIntervalMaps.isEmpty) NotFound
      else {
        (successfulIntervalMaps.head._1.hi, successfulIntervalMaps.last._1.hi) match {
          case (Finite(lastCompleteTime), Finite(lastTime)) =>
            Ok(
              Json.obj(
                "lastCompleteTime" -> lastCompleteTime.asJson,
                "lastTime" -> lastTime.asJson
              )
            )
          case _ => BadRequest
        }
      }

    case GET at url"/api/timeseries/backfills" =>
      Database
        .queryBackfills()
        .to[List]
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
  }: PartialService

  private[timeseries] def privateRoutes(): AuthenticatedService = {
    case req @ POST at url"/api/timeseries/backfill" => { implicit user =>
      req
        .readAs[Json]
        .flatMap(
          _.as[BackfillCreate]
            .fold(
              df =>
                IO.pure(
                  BadRequest(
                    s"""
                         |Error during backfill creation.
                         |Error: Cannot parse request body.
                         |$df
                         |""".stripMargin
                  )),
              backfill => {
                val jobIdsToBackfill = backfill.jobs.split(",").toSet
                scheduler
                  .backfillJob(
                    backfill.name,
                    backfill.description,
                    jobs.all.filter(job => jobIdsToBackfill.contains(job.id)),
                    backfill.startDate,
                    backfill.endDate,
                    backfill.priority,
                    executor.runningState,
                    xa
                  )
                  .map {
                    case Right(_)     => Ok("ok".asJson)
                    case Left(errors) => BadRequest(errors)
                  }
              }
            )
        )
    }

    // consider the given period of the job as successful, regardless of it's actual status
    case req @ GET at url"/api/timeseries/force-success?job=$jobId&start=$start&end=$end" => { implicit user =>
      (for {
        startDate <- Try(Instant.parse(start))
        endDate <- Try(Instant.parse(end))
        job <- Try(project.jobs.all.find(_.id == jobId).getOrElse(throw new Exception(s"Unknow job $jobId")))
      } yield {
        val requestedInterval = Interval(startDate, endDate)
        //self.forceSuccess(job, requestedInterval, executor.cuttleProject.version)
        scheduler.forceSuccess(job, requestedInterval, executor.projectVersion)
        val runningExecutions = executor.runningExecutions.filter(e => e._1.job.id == jobId).map(_._1)
        val failingExecutions = executor.allFailingExecutions.filter(_.job.id == jobId)
        val executions = runningExecutions ++ failingExecutions
        executions.foreach(_.cancel())
        (executions.length, JobSuccessForced(Instant.now(), user, jobId, startDate, endDate))
      }) match {
        case Success((canceledExecutions, event)) =>
          queries
            .logEvent(event)
            .transact(xa)
            .map(_ => Ok(Json.obj("canceled-executions" -> Json.fromInt(canceledExecutions))))
        case Failure(e) =>
          IO.pure(BadRequest(Json.obj("error" -> Json.fromString(e.getMessage))))
      }
    }
  }

  val api = publicApi orElse project.authenticator(privateApi)

  val publicAssets: PartialService = {
    case GET at url"/public/$file" =>
      ClasspathResource(s"/public/$file").fold(NotFound)(r => Ok(r))
  }

  val index: AuthenticatedService = {
    case req if req.url.startsWith("/api/") =>
      _ =>
        NotFound
    case _ =>
      _ =>
        Ok(ClasspathResource(s"/public/index.html"))
  }

  /** List of */
  val routes: PartialService = api
    .orElse(publicRoutes())
    .orElse(project.authenticator(privateRoutes()))
    .orElse {
      executor.platforms.foldLeft(PartialFunction.empty: PartialService) {
        case (s, p) => s.orElse(p.publicRoutes).orElse(project.authenticator(p.privateRoutes))
      }
    }
    .orElse(publicAssets orElse project.authenticator(index))
}
