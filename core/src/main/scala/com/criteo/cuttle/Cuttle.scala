package com.criteo.cuttle

import lol.http._
import scala.concurrent.ExecutionContext.Implicits.global

class CuttleProject[S <: Scheduling] private[cuttle] (
  val name: String,
  val version: String,
  val description: String,
  val env: (String, Boolean),
  val workflow: Workflow[S],
  val scheduler: Scheduler[S],
  val authenticator: Authenticator
) {
  def start(
    platforms: Seq[ExecutionPlatform] = CuttleProject.defaultPlatforms,
    httpPort: Int = 8888,
    databaseConfig: DatabaseConfig = DatabaseConfig.fromEnv,
    retryStrategy: RetryStrategy = RetryStrategy.ExponentialBackoffRetryStrategy
  ) = {
    val xa = Database.connect(databaseConfig)
    val executor = new Executor[S](platforms, xa)(retryStrategy)

    Server.listen(port = httpPort, onError = { e =>
      e.printStackTrace()
      InternalServerError(e.getMessage)
    })(App(this, executor, xa).routes)
    println(s"Listening on http://localhost:$httpPort")
    scheduler.start(workflow, executor, xa)
  }
}

object CuttleProject {
  def apply[S <: Scheduling](name: String,
                             version: String = "",
                             description: String = "",
                             env: (String, Boolean) = ("", false),
                             authenticator: Authenticator = GuestAuth)(workflow: Workflow[S])(
    implicit scheduler: Scheduler[S]): CuttleProject[S] =
    new CuttleProject(name, version, description, env, workflow, scheduler, authenticator)

  private[CuttleProject] def defaultPlatforms: Seq[ExecutionPlatform] = {
    import platforms._
    import java.util.concurrent.TimeUnit.{SECONDS}

    Seq(
      local.LocalPlatform(
        maxForkedProcesses = 10
      ),
      http.HttpPlatform(
        maxConcurrentRequests = 10,
        rateLimits = Seq(
          ".*" -> http.HttpPlatform.RateLimit(1, per = SECONDS)
        )
      )
    )
  }
}
