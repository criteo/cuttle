package com.criteo.cuttle

import lol.json._
import lol.http._

import io.circe._
import io.circe.syntax._

import java.time.{Instant}

import scala.util._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import Auth._
import ExecutionStatus._
import Metrics.{Gauge, Prometheus}
import utils.getJVMUptime

private[cuttle] object App {
  private implicit val S = fs2.Strategy.fromExecutionContext(global)
  private implicit val SC = fs2.Scheduler.fromFixedDaemonPool(1, "com.criteo.cuttle.App.SC")
  def sse[A](thunk: () => Option[A], encode: A => Json) = {
    val throttle = fs2.Stream.eval(fs2.Task.schedule((), 1.second))
    def next(previous: Option[A] = None): fs2.Stream[fs2.Task, ServerSentEvents.Event[Json]] =
      thunk()
        .filterNot(_ == previous.getOrElse(()))
        .map(a => fs2.Stream(ServerSentEvents.Event(encode(a))) ++ throttle.flatMap(_ => next(Some(a))))
        .getOrElse(throttle.flatMap(_ => next(previous)))
    thunk().map(_ => Ok(next().changes)).getOrElse(NotFound)
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

private[cuttle] case class App[S <: Scheduling](project: CuttleProject[S],
                                                executor: Executor[S],
                                                xa: XA,
                                                logger: Logger) {
  import App._
  import project.{scheduler, workflow}

  private def allJobs = workflow.vertices.map(_.id)

  private def extractJobs(jobsQueryString: String) =
    Try(jobsQueryString.split(",").filter(_.nonEmpty).toSet).filter(_.nonEmpty)

  private def extractJobsAndProcess(jobsQueryString: String)(process: Set[Job[S]] => Unit) = {
    val filteredJobs = extractJobs(jobsQueryString).getOrElse(Set.empty)

    if (filteredJobs.isEmpty) {
      process(workflow.vertices)
      Ok
    } else {
      lazy val jobsToProcess = workflow.vertices.filter(v => filteredJobs.contains(v.id))

      if (jobsToProcess.isEmpty) NotFound
      else {
        process(jobsToProcess)
        Ok
      }
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
      val filteredJobs = extractJobs(jobs).getOrElse(allJobs)

      def getStats() =
        Try(
          executor.getStats(filteredJobs) -> scheduler.getStats(filteredJobs)
        ).toOption

      def asJson(x: (Json, Json)) = x match {
        case (executorStats, schedulerStats) =>
          executorStats.deepMerge(
            Json.obj(
              "scheduler" -> schedulerStats
            ))
      }

      events match {
        case "true" | "yes" =>
          sse(getStats _, asJson)
        case _ =>
          getStats().map(stat => Ok(asJson(stat))).getOrElse(InternalServerError)
      }

    case GET at url"/api/statistics/$jobName" =>
      Ok(executor.jobStatsForLastThirtyDays(jobName).asJson)

    case GET at "/metrics" =>
      val metrics = executor.getMetrics(allJobs) ++ scheduler.getMetrics(allJobs) :+
        Gauge("cuttle_jvm_uptime_seconds").set(getJVMUptime)
      Ok(Prometheus.serialize(metrics))

    case GET at url"/api/executions/status/$kind?limit=$l&offset=$o&events=$events&sort=$sort&order=$a&jobs=$jobs" =>
      val limit = Try(l.toInt).toOption.getOrElse(25)
      val offset = Try(o.toInt).toOption.getOrElse(0)
      val asc = (a.toLowerCase == "asc")
      val filteredJobs = extractJobs(jobs).getOrElse(allJobs)

      def getExecutions() = kind match {
        case "started" =>
          Some(
            executor.runningExecutionsSizeTotal(filteredJobs) -> executor
              .runningExecutions(filteredJobs, sort, asc, offset, limit))
        case "stuck" =>
          Some(
            executor.failingExecutionsSize(filteredJobs) -> executor
              .failingExecutions(filteredJobs, sort, asc, offset, limit))
        case "paused" =>
          extractJobs(jobs)
          Some(
            executor.pausedExecutionsSize(filteredJobs) -> executor
              .pausedExecutions(filteredJobs, sort, asc, offset, limit))
        case "finished" =>
          Some(executor.archivedExecutionsSize(filteredJobs) -> executor.allRunning)
        case _ =>
          None
      }
      def asJson(x: (Int, Seq[ExecutionLog])) = x match {
        case (total, executions) =>
          Json.obj(
            "total" -> total.asJson,
            "offset" -> offset.asJson,
            "limit" -> limit.asJson,
            "sort" -> sort.asJson,
            "asc" -> asc.asJson,
            "data" -> (kind match {
              case "finished" =>
                executor.archivedExecutions(scheduler.allContexts, filteredJobs, sort, asc, offset, limit).asJson
              case _ =>
                executions.asJson
            })
          )
      }
      events match {
        case "true" | "yes" =>
          sse(getExecutions _, asJson)
        case _ =>
          getExecutions().map(e => Ok(asJson(e))).getOrElse(NotFound)
      }

    case GET at url"/api/executions/$id?events=$events" =>
      def getExecution() = executor.getExecution(scheduler.allContexts, id)
      events match {
        case "true" | "yes" =>
          sse(getExecution _, (e: ExecutionLog) => e.asJson)
        case _ =>
          getExecution().map(e => Ok(e.asJson)).getOrElse(NotFound)
      }

    case req @ GET at url"/api/executions/$id/streams" =>
      lazy val streams = executor.openStreams(id)
      req.headers.get(h"Accept").exists(_ == h"text/event-stream") match {
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

    case POST at url"/api/jobs/pause?jobs=$jobs" => { implicit user =>
      extractJobsAndProcess(jobs)(executor.pauseJobs)
    }

    case POST at url"/api/jobs/resume?jobs=$jobs" => { implicit user =>
      extractJobsAndProcess(jobs)(executor.resumeJobs)
    }

    case GET at url"/api/shutdown?gracePeriodSeconds=$gracePeriodSeconds" => { implicit user =>
      import scala.concurrent.duration._

      val gracePeriod: Try[Long] = gracePeriodSeconds match {
        case "" => Success(300)
        case p  => Try(p.toLong)
      }

      gracePeriod match {
        case Success(s) if s > 0 => {
          executor.gracefulShutdown(Duration(s, SECONDS))
          Ok
        }
        case Success(s) if s <= 0 => {
          executor.hardShutdown()
          Ok
        }
        case _ => BadRequest("gracePeriodSeconds should be an integer")
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
