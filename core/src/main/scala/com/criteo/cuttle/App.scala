package com.criteo.cuttle

import java.time.Instant

import scala.concurrent.duration._
import scala.util._

import cats.Eq
import cats.effect.IO
import cats.implicits._
import fs2.Stream
import io.circe._
import io.circe.syntax._
import lol.http._
import lol.json._

import com.criteo.cuttle.Auth._
import com.criteo.cuttle.ExecutionContexts.Implicits.serverExecutionContext
import com.criteo.cuttle.ExecutionContexts._
import com.criteo.cuttle.ExecutionStatus._
import com.criteo.cuttle.Metrics.{Gauge, Prometheus}
import com.criteo.cuttle.utils.getJVMUptime

private[cuttle] object App {
  private val SC = utils.createScheduler("com.criteo.cuttle.App.SC")

  def sse[A](thunk: IO[Option[A]], encode: A => IO[Json])(implicit eqInstance: Eq[A]): lol.http.Response = {
    val stream = (Stream.emit(()) ++ SC.fixedRate[IO](1.second))
      .evalMap(_ => IO.shift.flatMap(_ => thunk))
      .flatMap({
        case Some(x) => Stream(x)
        case None    => Stream.raiseError(new RuntimeException("Could not get result to stream"))
      })
      .changes
      .evalMap(r => encode(r))
      .map(ServerSentEvents.Event(_))

    Ok(stream)
  }

  implicit def projectEncoder[S <: Scheduling] = new Encoder[CuttleProject[S]] {
    override def apply(project: CuttleProject[S]) =
      Json.obj(
        "name" -> project.name.asJson,
        "version" -> Option(project.version).filterNot(_.isEmpty).asJson,
        "description" -> Option(project.description).filterNot(_.isEmpty).asJson,
        "env" -> Json.obj(
          "name" -> Option(project.env._1).filterNot(_.isEmpty).asJson,
          "critical" -> project.env._2.asJson
        )
      )
  }

  implicit val instantEncoder = new Encoder[Instant] {
    override def apply(date: Instant) =
      date.toString.asJson
  }

  implicit lazy val executionLogEncoder: Encoder[ExecutionLog] = new Encoder[ExecutionLog] {
    override def apply(execution: ExecutionLog) =
      Json.obj(
        "id" -> execution.id.asJson,
        "job" -> execution.job.asJson,
        "startTime" -> execution.startTime.asJson,
        "endTime" -> execution.endTime.asJson,
        "context" -> execution.context,
        "status" -> (execution.status match {
          case ExecutionSuccessful => "successful"
          case ExecutionFailed     => "failed"
          case ExecutionRunning    => "running"
          case ExecutionWaiting    => "waiting"
          case ExecutionPaused     => "paused"
          case ExecutionThrottled  => "throttled"
          case ExecutionTodo       => "todo"
        }).asJson,
        "failing" -> execution.failing.map {
          case FailingJob(failedExecutions, nextRetry) =>
            Json.obj(
              "failedExecutions" -> Json.fromValues(failedExecutions.map(_.asJson(executionLogEncoder))),
              "nextRetry" -> nextRetry.asJson
            )
        }.asJson,
        "waitingSeconds" -> execution.waitingSeconds.asJson
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

  implicit def jobEncoder[S <: Scheduling] = new Encoder[Job[S]] {
    override def apply(job: Job[S]) =
      Json
        .obj(
          "id" -> job.id.asJson,
          "name" -> Option(job.name).filterNot(_.isEmpty).getOrElse(job.id).asJson,
          "description" -> Option(job.description).filterNot(_.isEmpty).asJson,
          "scheduling" -> job.scheduling.toJson,
          "tags" -> job.tags.map(_.name).asJson
        )
        .asJson
  }

  implicit def workflowEncoder[S <: Scheduling] =
    new Encoder[Workflow[S]] {
      override def apply(workflow: Workflow[S]) = {
        val jobs = workflow.jobsInOrder.asJson
        val tags = workflow.vertices.flatMap(_.tags).asJson
        val dependencies = workflow.edges.map {
          case (to, from, _) =>
            Json.obj(
              "from" -> from.id.asJson,
              "to" -> to.id.asJson
            )
        }.asJson
        Json.obj(
          "jobs" -> jobs,
          "dependencies" -> dependencies,
          "tags" -> tags
        )
      }
    }
}

private[cuttle] case class App[S <: Scheduling](project: CuttleProject[S], executor: Executor[S], xa: XA) {
  import project.{scheduler, workflow}

  import App._

  private val allIds = workflow.vertices.map(_.id)

  private def parseJobIds(jobsQueryString: String): Set[String] =
    jobsQueryString.split(",").filter(_.trim().nonEmpty).toSet

  private def getJobsOrNotFound(jobsQueryString: String): Either[Response, Set[Job[S]]] = {
    val jobsNames = parseJobIds(jobsQueryString)
    if (jobsNames.isEmpty) Right(workflow.vertices)
    else {
      val jobs = workflow.vertices.filter(v => jobsNames.contains(v.id))
      if (jobs.isEmpty) Left(NotFound)
      else Right(jobs)
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

    case GET at "/metrics" =>
      val metrics =
        executor.getMetrics(allIds, workflow) ++
          scheduler.getMetrics(allIds, workflow) :+
          Gauge("cuttle_jvm_uptime_seconds").set(getJVMUptime)
      Ok(Prometheus.serialize(metrics))

    case GET at url"/api/executions/status/$kind?limit=$l&offset=$o&events=$events&sort=$sort&order=$a&jobs=$jobs" =>
      val jobIds = parseJobIds(jobs)
      val limit = Try(l.toInt).toOption.getOrElse(25)
      val offset = Try(o.toInt).toOption.getOrElse(0)
      val asc = a.toLowerCase == "asc"
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
        case "paused" =>
          IO(
            Some(
              executor.pausedExecutionsSize(ids) -> executor
                .pausedExecutions(ids, sort, asc, offset, limit)
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
      Ok(executor.pausedJobs.asJson)

    case GET at "/api/project_definition" =>
      Ok(project.asJson)

    case GET at "/api/workflow_definition" =>
      Ok(workflow.asJson)
  }

  val privateApi: AuthenticatedService = {
    case POST at url"/api/executions/$id/cancel" => { implicit user =>
      executor.cancelExecution(id)
      Ok
    }

    case POST at url"/api/executions/$id/force/success" => { implicit user =>
      executor.forceSuccess(id)
      IO.pure(Ok)
    }

    case POST at url"/api/jobs/pause?jobs=$jobs" => { implicit user =>
      getJobsOrNotFound(jobs).fold(IO.pure, jobs => {
        executor.pauseJobs(jobs)
        Ok
      })
    }

    case POST at url"/api/jobs/resume?jobs=$jobs" => { implicit user =>
      getJobsOrNotFound(jobs).fold(IO.pure, jobs => {
        executor.resumeJobs(jobs)
        Ok
      })
    }
    case POST at url"/api/jobs/all/unpause" => { implicit user =>
      executor.resumeJobs(workflow.vertices)
      Ok
    }
    case POST at url"/api/jobs/$id/unpause" => { implicit user =>
      workflow.vertices.find(_.id == id).fold(NotFound) { job =>
        executor.resumeJobs(Set(job))
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

    case req@GET at url"/api/shutdown" => { implicit user =>
      import scala.concurrent.duration._

      req.queryStringParameters.get("gracePeriodSeconds") match {
        case Some(s) =>
          Try(s.toLong) match {
            case Success(s) if s > 0 =>
              executor.gracefulShutdown(Duration(s, SECONDS))
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

  val routes: Service = api
    .orElse(scheduler.publicRoutes(workflow, executor, xa))
    .orElse(project.authenticator(scheduler.privateRoutes(workflow, executor, xa)))
    .orElse {
      executor.platforms.foldLeft(PartialFunction.empty: PartialService) {
        case (s, p) => s.orElse(p.publicRoutes).orElse(project.authenticator(p.privateRoutes))
      }
    }
    .orElse(publicAssets orElse project.authenticator(index))
}
