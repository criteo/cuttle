package com.criteo.langoustinepp

import lol.http._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class LangoustinePP() {

  val routes: Request => Future[Response] = {
    case GET at "/" =>
      Redirect("/ping")

    case GET at "/ping" =>
      Ok("pong!")
  }
}

object LangoustinePP {

  def apply(): LangoustinePP = {
    new LangoustinePP()
  }
}

object LangoustinePPServer {

  private val langoustinepp = LangoustinePP()

  def main(args: Array[String]): Unit = {
    val httpPort = args.lift(0).map(_.toInt).getOrElse(8888)
    Server.listen(
      port = httpPort,
      onError = { e =>
        e.printStackTrace()
        InternalServerError("LOL.")
      })(langoustinepp.routes)
    println(s"Listening on http://localhost:$httpPort")
  }
}