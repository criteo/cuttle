package org.criteo.langoustine

import lol.http._
import doobie.imports._

case class App[S <: Scheduling](workflow: Graph[S], scheduler: Scheduler[S], executor: Executor[S], database: Database) {
  private val main: PartialService = {
    case GET at "/" =>
      Ok("Hello langoustine!")

    case GET at url"/wat/$number" =>
      val dataFromDatabase = database.run {
        for {
          a <- sql"select 42".query[Int].unique
          b <- sql"select random()".query[Double].unique
          c <- sql"select 'yop' where cast($number AS int) > 10".query[String].option
        } yield (a, b, c)
      }
      Ok(s"=> $dataFromDatabase")
  }

  private val notFound: PartialService = {
    case _ =>
      NotFound("Page not found")
  }

  lazy val routes = main.orElse(scheduler.routes).orElse(notFound)
}