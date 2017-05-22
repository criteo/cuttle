package com.criteo.cuttle

import lol.json._
import lol.http._
import JsonApi._

import io.circe._
import io.circe.syntax._

import scala.util._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

case class App[S <: Scheduling](project: Project, workflow: Graph[S], scheduler: Scheduler[S], executor: Executor[S]) {

  private implicit val S = fs2.Strategy.fromExecutionContext(global)
  private implicit val SC = fs2.Scheduler.fromFixedDaemonPool(1, "com.criteo.cuttle.App.SC")
  private def sse[A](thunk: () => A, encode: A => Json) = {
    val throttle = fs2.Stream.eval(fs2.Task.schedule((), 1.second))
    def next(previous: Option[A] = None): fs2.Stream[fs2.Task, ServerSentEvents.Event[Json]] =
      Option(thunk())
        .filterNot(_ == previous.getOrElse(()))
        .map(a => fs2.Stream(ServerSentEvents.Event(encode(a))) ++ throttle.flatMap(_ => next(Some(a))))
        .getOrElse(throttle.flatMap(_ => next(previous)))
    Ok(next())
  }

  val api: PartialService = {

    case GET at url"/api/statistics?stream=$stream" =>
      def getStats() =
        (executor.runningExecutionsSize, executor.pausedExecutionsSize, executor.failingExecutionsSize)
      def asJson(x: (Int, Int, Int)) = x match {
        case (running, paused, failing) =>
          Json.obj(
            "running" -> running.asJson,
            "paused" -> paused.asJson,
            "failing" -> failing.asJson
          )
      }
      stream match {
        case "true" | "yes" =>
          sse(getStats, asJson)
        case _ =>
          Ok(asJson(getStats()))
      }

    case GET at url"/api/executions/$kind?limit=$l&offset=$o&stream=$stream&sort=$sort&order=$a" =>
      val limit = Try(l.toInt).toOption.getOrElse(25)
      val offset = Try(o.toInt).toOption.getOrElse(0)
      val asc = (a.toLowerCase == "asc")
      def getExecutions() = kind match {
        case "started" =>
          (executor.runningExecutionsSize, executor.runningExecutions(sort, asc, offset, limit))
        case "stuck" =>
          (executor.failingExecutionsSize, executor.failingExecutions(sort, asc, offset, limit))
        case "paused" =>
          (executor.pausedExecutionsSize, executor.pausedExecutions(sort, asc, offset, limit))
        case "finished" =>
          (executor.archivedExecutionsSize, executor.rawRunningExecutions)
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
                executor.archivedExecutions(scheduler.allContexts, sort, asc, offset, limit).asJson
              case _ =>
                executions.asJson
            })
          )
      }
      stream match {
        case "true" | "yes" =>
          sse(getExecutions, asJson)
        case _ =>
          Ok(asJson(getExecutions()))
      }

    case POST at url"/api/executions/$id/cancel" =>
      executor.cancelExecution(id)
      Ok

    case request @ GET at url"/api/executions/$id/streams" =>
      lazy val flush = fs2.Stream.chunk(fs2.Chunk.bytes((" " * 5 * 1024).getBytes))
      lazy val pre = fs2.Stream("<pre>".getBytes: _*)
      lazy val logs = executor.openStreams(id)
      Ok(
        if (request.queryString.exists(_ == "html"))
          Content(
            stream = flush ++ pre ++ logs,
            headers = Map(h"Content-Type" -> h"text/html")
          )
        else
          Content(
            stream = logs,
            headers = Map(h"Content-Type" -> h"text/plain")
          )
      )

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
    .orElse(scheduler.routes(workflow, executor))
    .orElse {
      executor.platforms.foldLeft(PartialFunction.empty: PartialService) { case (s, p) => s.orElse(p.routes) }
    }
    .orElse(webapp)
}
