package com.criteo.cuttle.timeseries

import lol.http._

import com.criteo.cuttle._
import com.criteo.cuttle.{Database => CuttleDatabase}
import com.criteo.cuttle.ThreadPools._, Implicits.serverThreadPool

/**
  * A cuttle project is a workflow to execute with the appropriate scheduler.
  * See the [[CuttleProject]] companion object to create projects.
  */
class CuttleProject private[cuttle] (
    val name: String,
    val version: String,
    val description: String,
    val env: (String, Boolean),
    val jobs: Workflow,
    val scheduler: TimeSeriesScheduler,
    val authenticator: Auth.Authenticator,
    val logger: Logger) {

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
    retryStrategy: RetryStrategy = RetryStrategy.ExponentialBackoffRetryStrategy,
    paused: Boolean = false
  ): Unit = {
    val xa = CuttleDatabase.connect(databaseConfig)
    val executor = new Executor[TimeSeries](platforms, xa, logger, version)(retryStrategy)

    if (paused) {
      logger.info("Pausing workflow")
      executor.pauseJobs(jobs.all)(Auth.User("Startup"))
    }

    logger.info("Start workflow")
    scheduler.start(jobs, executor, xa, logger)

    logger.info("Start server")
    Server.listen(port = httpPort, onError = { e =>
      e.printStackTrace()
      InternalServerError(e.getMessage)
    })(TimeSeriesApp(this, executor, xa).routes)

    logger.info(s"Listening on http://localhost:$httpPort")
  }

  /**
    * Connect to database and build routes. It allows you to start externally a server and decide when to start the scheduling.
    *
    * @param platforms The configured [[ExecutionPlatform ExecutionPlatforms]] to use to execute jobs.
    * @param databaseConfig JDBC configuration for MySQL server 5.7.
    * @param retryStrategy The strategy to use for execution retry. Default to exponential backoff.
    *
    * @return a tuple with cuttleRoutes (needed to start a server) and a function to start the scheduler
    */
  def build(
    platforms: Seq[ExecutionPlatform] = CuttleProject.defaultPlatforms,
    databaseConfig: DatabaseConfig = DatabaseConfig.fromEnv,
    retryStrategy: RetryStrategy = RetryStrategy.ExponentialBackoffRetryStrategy
  ): (Service, () => Unit) = {
    val xa = CuttleDatabase.connect(databaseConfig)
    val executor = new Executor[TimeSeries](platforms, xa, logger, version)(retryStrategy)

    val startScheduler = () => {
      logger.info("Start workflow")
      scheduler.start(jobs, executor, xa, logger)
    }

    (TimeSeriesApp(this, executor, xa).routes, startScheduler)
  }
}

/**
  * Create new projects using a timeseries scheduler.
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
    * @param jobs The workflow to run in this project.
    * @param scheduler The scheduler instance to use to schedule the Workflow jobs.
    * @param logger The logger to use to log internal debug informations.
    */
  def apply(name: String,
            version: String = "",
            description: String = "",
            env: (String, Boolean) = ("", false),
            authenticator: Auth.Authenticator = Auth.GuestAuth)(
            jobs: Workflow)(implicit scheduler: TimeSeriesScheduler, logger: Logger): CuttleProject =
    new CuttleProject(name, version, description, env, jobs, scheduler, authenticator, logger)

  private[CuttleProject] def defaultPlatforms: Seq[ExecutionPlatform] = {
    import java.util.concurrent.TimeUnit.SECONDS

    import platforms._

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
