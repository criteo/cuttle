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
  private def sse[A](thunk: () => Option[A], encode: A => Json) = {
    val throttle = fs2.Stream.eval(fs2.Task.schedule((), 1.second))
    def next(previous: Option[A] = None): fs2.Stream[fs2.Task, ServerSentEvents.Event[Json]] =
      thunk()
        .filterNot(_ == previous.getOrElse(()))
        .map(a => fs2.Stream(ServerSentEvents.Event(encode(a))) ++ throttle.flatMap(_ => next(Some(a))))
        .getOrElse(throttle.flatMap(_ => next(previous)))
    thunk().map(_ => Ok(next())).getOrElse(NotFound)
  }

  val api: PartialService = {

    case GET at url"/api/statistics?events=$events" =>
      def getStats() =
        Some((executor.runningExecutionsSize, executor.pausedExecutionsSize, executor.failingExecutionsSize))
      def asJson(x: (Int, Int, Int)) = x match {
        case (running, paused, failing) =>
          Json.obj(
            "running" -> running.asJson,
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

    case GET at url"/api/executions/status/$kind?limit=$l&offset=$o&events=$events&sort=$sort&order=$a" =>
      val limit = Try(l.toInt).toOption.getOrElse(25)
      val offset = Try(o.toInt).toOption.getOrElse(0)
      val asc = (a.toLowerCase == "asc")
      def getExecutions() = kind match {
        case "started" =>
          Some(executor.runningExecutionsSize -> executor.runningExecutions(sort, asc, offset, limit))
        case "stuck" =>
          Some(executor.failingExecutionsSize -> executor.failingExecutions(sort, asc, offset, limit))
        case "paused" =>
          Some(executor.pausedExecutionsSize -> executor.pausedExecutions(sort, asc, offset, limit))
        case "finished" =>
          Some(executor.archivedExecutionsSize -> executor.rawRunningExecutions)
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
                executor.archivedExecutions(scheduler.allContexts, sort, asc, offset, limit).asJson
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
    .orElse(scheduler.routes(workflow))
    .orElse {
      executor.platforms.foldLeft(PartialFunction.empty: PartialService) { case (s, p) => s.orElse(p.routes) }
    }
    .orElse(webapp)
}
