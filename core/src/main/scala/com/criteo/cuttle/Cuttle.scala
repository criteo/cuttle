package com.criteo.cuttle

import lol.http._
import scala.concurrent.ExecutionContext.Implicits.global
import com.criteo.cuttle.platforms.local.LocalPlatform

class CuttleProject[S <: Scheduling] private[cuttle] (
  val name: String,
  val description: String,
  val env: (String, Boolean),
  val workflow: Workflow[S],
  val scheduler: Scheduler[S],
  val ordering: Ordering[S#Context],
  val retryStrategy: RetryStrategy[S],
  queries: Queries
) {
  def start(
    platforms: Seq[ExecutionPlatform[S]] = List(LocalPlatform(maxForkedProcesses = 10)(ordering)),
    httpPort: Int = 8888,
    databaseConfig: DatabaseConfig = DatabaseConfig.fromEnv
  ) = {
    val xa = Database.connect(databaseConfig)
    val executor = new Executor[S](platforms, queries, xa)(retryStrategy, ordering)
    Server.listen(port = httpPort, onError = { e =>
      e.printStackTrace()
      InternalServerError(e.getMessage)
    })(App(this, executor, xa).routes)
    println(s"Listening on http://localhost:$httpPort")
    scheduler.start(workflow, executor, xa)
  }
}

object CuttleProject {
  def apply[S <: Scheduling](name: String, description: String = "", env: (String, Boolean) = ("", false))(
    workflow: Workflow[S])(implicit scheduler: Scheduler[S],
                           retryStrategy: RetryStrategy[S],
                           ordering: Ordering[S#Context]): CuttleProject[S] =
    new CuttleProject(name, description, env, workflow, scheduler, ordering, retryStrategy, new Queries {})
}
