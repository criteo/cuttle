package org.criteo.langoustine

import lol.http._

object App {

  val routes: PartialService = {
    case GET at "/" =>
      Redirect("/ping")

    case GET at "/ping" =>
      Ok("pong!!!")
  }

}