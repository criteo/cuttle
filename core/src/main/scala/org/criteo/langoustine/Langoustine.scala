package org.criteo.langoustine

import lol.http._

import scala.concurrent.ExecutionContext.Implicits.global

class Langoustine[S <: Scheduling](
  workflow: Graph[S],
  scheduler: Scheduler[S],
  ordering: Ordering[S#Context],
  database: Database
) {
  def run(
    platforms: Seq[ExecutionPlatform[S]] = List(LocalPlatform(maxTasks = 10)(ordering)),
    httpPort: Int = 8888
  ) = {
    val executor = Executor[S](platforms)
    Server.listen(port = httpPort, onError = { e =>
      e.printStackTrace()
      InternalServerError("LOL")
    })(App(workflow, scheduler, executor, database).routes)
    println(s"Listening on http://localhost:$httpPort")
    scheduler.run(workflow, executor)
  }
}

object Langoustine {
  def apply[S <: Scheduling](workflow: Graph[S], database: DatabaseConfig = Database.configFromEnv)(
    implicit scheduler: Scheduler[S],
    ordering: Ordering[S#Context]): Langoustine[S] =
    new Langoustine(workflow, scheduler, ordering, Database.connect(database))
}
