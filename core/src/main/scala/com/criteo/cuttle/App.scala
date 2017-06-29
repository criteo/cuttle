package com.criteo.cuttle

import lol.json._
import lol.http._

import io.circe._
import io.circe.syntax._

import java.time.{Instant}

import scala.util._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import ExecutionStatus._

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
          case ExecutionFailed => "failed"
          case ExecutionRunning => "running"
          case ExecutionWaiting => "waiting"
          case ExecutionPaused => "paused"
          case ExecutionThrottled => "throttled"
          case ExecutionTodo => "todo"
        }).asJson,
        "failing" -> execution.failing.map {
          case FailingJob(failedExecutions, nextRetry) =>
            Json.obj(
              "failedExecutions" -> Json.fromValues(failedExecutions.map(_.asJson(executionLogEncoder))),
              "nextRetry" -> nextRetry.asJson
            )
        }.asJson
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
  import App._
  import project.{scheduler, workflow}

  val api: PartialService = {

    case GET at url"/api/statistics?events=$events&jobs=$jobs" =>
      val filteredJobs = Try(jobs.split(",").toSeq.filter(_.nonEmpty)).toOption
        .filter(_.nonEmpty)
        .getOrElse(workflow.vertices.map(_.id))
        .toSet
      def getStats() =
        Some(
          (executor.runningExecutionsSizes(filteredJobs),
           executor.pausedExecutionsSize(filteredJobs),
           executor.failingExecutionsSize(filteredJobs)))
      def asJson(x: ((Int, Int), Int, Int)) = x match {
        case ((running, waiting), paused, failing) =>
          Json.obj(
            "running" -> running.asJson,
            "waiting" -> waiting.asJson,
            "paused" -> paused.asJson,
            "failing" -> failing.asJson
          )
      }
      events match {
        case "true" | "yes" =>
          sse(getStats, asJson)
        case _ =>
          Ok(asJson(getStats().get))
      }

    case GET at url"/api/executions/status/$kind?limit=$l&offset=$o&events=$events&sort=$sort&order=$a&jobs=$jobs" =>
      val limit = Try(l.toInt).toOption.getOrElse(25)
      val offset = Try(o.toInt).toOption.getOrElse(0)
      val asc = (a.toLowerCase == "asc")
      val filteredJobs = Try(jobs.split(",").toSeq.filter(_.nonEmpty)).toOption
        .filter(_.nonEmpty)
        .getOrElse(workflow.vertices.map(_.id))
        .toSet
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
          sse(getExecutions, asJson)
        case _ =>
          getExecutions().map(e => Ok(asJson(e))).getOrElse(NotFound)
      }

    case GET at url"/api/executions/$id?events=$events" =>
      def getExecution() = executor.getExecution(scheduler.allContexts, id)
      events match {
        case "true" | "yes" =>
          sse(getExecution, (e: ExecutionLog) => e.asJson)
        case _ =>
          getExecution().map(e => Ok(e.asJson)).getOrElse(NotFound)
      }

    case POST at url"/api/executions/$id/cancel" =>
      executor.cancelExecution(id)
      Ok

    case GET at url"/api/executions/$id/streams?events=$events" =>
      lazy val streams = executor.openStreams(id)
      events match {
        case "true" | "yes" =>
          Ok(
            streams
              .through(fs2.text.utf8Decode)
              .through(fs2.text.lines)
              .chunks
              .map(chunk => ServerSentEvents.Event(Json.fromValues(chunk.toArray.toIterable.map(_.asJson)))) ++
              fs2.Stream(ServerSentEvents.Event("EOS".asJson))
          )
        case _ =>
          Ok(
            Content(
              stream = streams,
              headers = Map(h"Content-Type" -> h"text/plain")
            ))
      }

    case GET at url"/api/jobs/paused" =>
      Ok(executor.pausedJobs.asJson)

    case POST at url"/api/jobs/all/pause" =>
      executor.pauseJobs(workflow.vertices)
      Ok

    case POST at url"/api/jobs/$id/pause" =>
      workflow.vertices.find(_.id == id).fold(NotFound) { job =>
        executor.pauseJobs(Set(job))
        Ok
      }

    case POST at url"/api/jobs/all/unpause" =>
      executor.unpauseJobs(workflow.vertices)
      Ok

    case POST at url"/api/jobs/$id/unpause" =>
      workflow.vertices.find(_.id == id).fold(NotFound) { job =>
        executor.unpauseJobs(Set(job))
        Ok
      }

    case GET at "/api/project_definition" =>
      Ok(project.asJson)

    case GET at "/api/workflow_definition" =>
      Ok(workflow.asJson)
  }

  val webapp: PartialService = {
    case GET at url"/public/$file" =>
      ClasspathResource(s"/public/$file").fold(NotFound)(r => Ok(r))

    case req if req.url.startsWith("/api/") =>
      NotFound

    case _ =>
      Ok(ClasspathResource(s"/public/index.html"))
  }

  val routes = api
    .orElse(scheduler.routes(workflow, executor, xa))
    .orElse {
      executor.platforms.foldLeft(PartialFunction.empty: PartialService) { case (s, p) => s.orElse(p.routes) }
    }
    .orElse(webapp)
}
