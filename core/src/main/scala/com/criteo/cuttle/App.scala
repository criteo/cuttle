package com.criteo.cuttle

import lol.json._
import lol.http._
import io.circe.syntax._
import Api._

import scala.concurrent.ExecutionContext.Implicits.global

case class App[S <: Scheduling](project: Project, workflow: Graph[S], scheduler: Scheduler[S], executor: Executor[S]) {
  private val main: PartialService = {

    case POST at url"/api/pause/$id" =>
      executor.pauseJob(workflow.vertices.find(_.id == id).get)
      Ok("paused")

    case POST at url"/api/unpause/$id" =>
      executor.unpauseJob(workflow.vertices.find(_.id == id).get)
      Ok("unpaused")

    case GET at "/api/project_definition" =>
      Ok(project.asJson)

    case GET at "/api/workflow_definition" =>
      Ok(workflow.asJson)

    case GET at url"/public/$file" =>
      ClasspathResource(s"/public/$file").fold(NotFound)(r => Ok(r))

    case req => {
      if (req.url.startsWith("/api"))
        NotFound("Api endpoint not found")
      else
        Ok(ClasspathResource(s"/public/index.html"))
    }
  }

  lazy val routes = main.orElse(scheduler.routes)
}


