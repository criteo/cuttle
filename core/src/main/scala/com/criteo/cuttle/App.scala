package com.criteo.cuttle

import lol.html._
import lol.json._
import lol.http._
import io.circe.syntax._
import Api._

case class App[S <: Scheduling](project: Project, workflow: Graph[S], scheduler: Scheduler[S], executor: Executor[S]) {
  private val main: PartialService = {
    case GET at "/" =>
      val successfulExecutions = executor.getExecutionLog(true)
      Ok(
        html"""
          <h1>Successful executions</h1>
          <ul>
          ${successfulExecutions.map { e =>
          html"<li>$e</li>"
        }}
          </ul>
        """
      )

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
  }

  private val notFound: PartialService = {
    case _ =>
      NotFound("Page not found")
  }
  lazy val routes = main.orElse(scheduler.routes).orElse(notFound)
}
