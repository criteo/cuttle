package org.criteo.langoustine

import lol.http._
import lol.html._

case class App[S <: Scheduling](workflow: Graph[S], scheduler: Scheduler[S], executor: Executor[S]) {
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
  }

  private val notFound: PartialService = {
    case _ =>
      NotFound("Page not found")
  }

  lazy val routes = main.orElse(scheduler.routes).orElse(notFound)
}
