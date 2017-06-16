package com.criteo.cuttle

import lol.http._
import scala.concurrent.ExecutionContext.Implicits.global

class CuttleProject[S <: Scheduling] private[cuttle] (
  val name: String,
  val description: String,
  val env: (String, Boolean),
  val workflow: Workflow[S],
  val scheduler: Scheduler[S],
  val ordering: Ordering[S#Context],
  val retryStrategy: RetryStrategy[S]
) {
  def start(
    platforms: Seq[ExecutionPlatform[S]] = CuttleProject.defaultPlatforms(ordering),
    httpPort: Int = 8888,
    databaseConfig: DatabaseConfig = DatabaseConfig.fromEnv
  ) = {
    val xa = Database.connect(databaseConfig)
    val executor = new Executor[S](platforms, xa)(retryStrategy, ordering)
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
    new CuttleProject(name, description, env, workflow, scheduler, ordering, retryStrategy)

  private[CuttleProject] def defaultPlatforms[S <: Scheduling](ordering: Ordering[S#Context]) = {
    import platforms._
    import java.util.concurrent.TimeUnit.{SECONDS}

    Seq(
      local.LocalPlatform(
        maxForkedProcesses = 10
      )(ordering),
      http.HttpPlatform(
        maxConcurrentRequests = 10,
        rateLimits = Seq(
          ".*" -> http.HttpPlatform.RateLimit(1, per = SECONDS)
        )
      )(ordering)
    )
  }
}
