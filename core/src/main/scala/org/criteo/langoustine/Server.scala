package org.criteo.langoustine

import lol.http._

import scala.concurrent.ExecutionContext.Implicits.global

object Langoustine {
  val routes: PartialService = {
    case GET at "/" =>
      Redirect("/ping")

    case GET at "/ping" =>
      Ok("pong!!!")
  }

  def run[S <: Scheduling](worflow: Graph[S], httpPort: Int = 8888)(implicit scheduler: Scheduler[S]) = {
    Server.listen(
      port = httpPort,
      onError = { e =>
        e.printStackTrace()
        InternalServerError("LOL.")
      })(routes)
    println(s"Listening on http://localhost:$httpPort")
  }
}