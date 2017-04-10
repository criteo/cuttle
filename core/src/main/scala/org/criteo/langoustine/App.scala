package org.criteo.langoustine

import lol.http._

case class App[S <: Scheduling](scheduler: Scheduler[S], executor: Executor[S]) {
  private val main: PartialService = {
    case GET at "/" =>
      Ok("Hello langoustine!")
  }

  private val notFound: PartialService = {
    case _ =>
      NotFound("Page not found")
  }

  lazy val routes = main.orElse(scheduler.routes).orElse(notFound)
}