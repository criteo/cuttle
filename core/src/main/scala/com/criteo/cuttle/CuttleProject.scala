package com.criteo.cuttle

import lol.http._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * A cuttle project is a workflow to execute with the appropriate scheduler.
  * See the [[CuttleProject]] companion object to create projects.
  *
  * @tparam S The type of [[Scheduling]] used by the project (for example [[timeseries.TimeSeries TimeSeries]]).
  */
class CuttleProject[S <: Scheduling] private[cuttle] (
  val name: String,
  val version: String,
  val description: String,
  val env: (String, Boolean),
  val workflow: Workflow[S],
  val scheduler: Scheduler[S],
  val authenticator: Auth.Authenticator,
  val logger: Logger
) {

  /**
    * Start scheduling and execution with the given environment. It also starts
    * an HTTP server providing an Web UI and a JSON API.
    *
    * @param platforms The configured [[ExecutionPlatform ExecutionPlatforms]] to use to execute jobs.
    * @param httpPort The port to use for the HTTP daemon.
    * @param databaseConfig JDBC configuration for MySQL server 5.7.
    * @param retryStrategy The strategy to use for execution retry. Default to exponential backoff.
    */
  def start(
    platforms: Seq[ExecutionPlatform] = CuttleProject.defaultPlatforms,
    httpPort: Int = 8888,
    databaseConfig: DatabaseConfig = DatabaseConfig.fromEnv,
    retryStrategy: RetryStrategy = RetryStrategy.ExponentialBackoffRetryStrategy
  ): Unit = {
    val xa = Database.connect(databaseConfig)
    val executor = new Executor[S](platforms, xa, logger = logger, cuttleProject = this)(retryStrategy)

    logger.info("Start workflow")
    scheduler.start(workflow, executor, xa, logger)

    logger.info("Start server")
    Server.listen(port = httpPort, onError = { e =>
      e.printStackTrace()
      InternalServerError(e.getMessage)
    })(App(this, executor, xa, logger).routes)

    logger.info(s"Listening on http://localhost:$httpPort")
  }
}

/**
  * Create new projects.
  * @tparam S The type of scheduling for the project.
  */
object CuttleProject {

  /**
    * Create a new project.
    * @param name The project name as displayed in the UI.
    * @param version The project version as displayed in the UI.
    * @param description The project version as displayed in the UI.
    * @param env The environment as displayed in the UI (The string is the name while the boolean indicates
    *            if the environment is a production one).
    * @param authenticator The way to authenticate HTTP request for the UI and the private API.
    * @param workflow The workflow to run in this project.
    * @param scheduler The scheduler instance to use to schedule the Workflow jobs.
    * @param logger The logger to use to log internal debug informations.
    */
  def apply[S <: Scheduling](name: String,
                             version: String = "",
                             description: String = "",
                             env: (String, Boolean) = ("", false),
                             authenticator: Auth.Authenticator = Auth.GuestAuth)(
    workflow: Workflow[S])(implicit scheduler: Scheduler[S], logger: Logger): CuttleProject[S] =
    new CuttleProject(name, version, description, env, workflow, scheduler, authenticator, logger)

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
