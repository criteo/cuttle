package com.criteo.cuttle

import lol.http._
import scala.concurrent.ExecutionContext.Implicits.global

class CuttleProject[S <: Scheduling](
  val name: String,
  val description: String,
  val env: (String, Boolean),
  val workflow: Graph[S],
  val scheduler: Scheduler[S],
  val ordering: Ordering[S#Context],
  retryStrategy: RetryStrategy[S],
  queries: Queries
) {
  def run(
    platforms: Seq[ExecutionPlatform[S]] = List(LocalPlatform(maxForkedProcesses = 10)(ordering)),
    httpPort: Int = 8888,
    databaseConfig: DatabaseConfig = Database.configFromEnv
  ) = {
    val xa = Database.connect(databaseConfig)
    val executor = Executor[S](platforms, queries, xa)(retryStrategy, ordering)
    Server.listen(port = httpPort, onError = { e =>
      e.printStackTrace()
      InternalServerError(e.getMessage)
    })(App(this, executor, xa).routes)
    println(s"Listening on http://localhost:$httpPort")
    scheduler.run(workflow, executor, xa)
  }
}

object CuttleProject {
  def apply[S <: Scheduling](name: String, description: String = "", env: (String, Boolean) = ("", false))(workflow: Graph[S])(
    implicit scheduler: Scheduler[S],
    retryStrategy: RetryStrategy[S],
    ordering: Ordering[S#Context]): CuttleProject[S] =
    new CuttleProject(name, description, env, workflow, scheduler, ordering, retryStrategy, new Queries {})
}
