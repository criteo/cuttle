package com.criteo.cuttle

import lol.json._
import lol.http._
import JsonApi._

import io.circe.syntax._

import scala.concurrent.ExecutionContext.Implicits.global

case class App[S <: Scheduling](project: Project, workflow: Graph[S], scheduler: Scheduler[S], executor: Executor[S]) {

  val api: PartialService = {
    case GET at url"/api/executions/running" =>
      Ok(executor.runningExecutions.asJson)

    case GET at url"/api/executions/paused" =>
      Ok(executor.pausedExecutions.asJson)

    case GET at url"/api/executions/archived" =>
      Ok(executor.archivedExecutions.asJson)

    case POST at url"/api/executions/$id/cancel" =>
      executor.cancelExecution(id)
      Ok

    case GET at url"/api/jobs/paused" =>
      Ok(executor.pausedJobs.asJson)

    case POST at url"/api/jobs/$id/pause" =>
      workflow.vertices.find(_.id == id).fold(NotFound) { job =>
        executor.pauseJob(job)
        Ok
      }

    case POST at url"/api/jobs/$id/unpause" =>
      workflow.vertices.find(_.id == id).fold(NotFound) { job =>
        executor.unpauseJob(job)
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

  val routes = api.orElse(scheduler.routes).orElse {
    executor.platforms.foldLeft(PartialFunction.empty:PartialService) { case (s, p) => s.orElse(p.routes) }
  }.orElse(webapp)
}
