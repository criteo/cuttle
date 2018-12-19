package com.criteo.cuttle.cron

import scala.concurrent.duration._

import lol.http._

import com.criteo.cuttle._
import com.criteo.cuttle.ThreadPools._, Implicits.serverThreadPool

/**
  * A cuttle project is a workflow to execute with the appropriate scheduler.
  * See the [[CuttleProject]] companion object to create projects.
  */
case class CuttleProject[S <: Scheduling] private[cuttle] (name: String,
                                                           version: String,
                                                           description: String,
                                                           env: (String, Boolean),
                                                           jobs: Workload[S],
                                                           scheduler: Scheduler[S],
                                                           routes: PartialService,
                                                           logger: Logger) {

  /**
    * Start scheduling and execution with the given environment. It also starts
    * an HTTP server providing an Web UI and a JSON API.
    *
    * @param platforms The configured [[ExecutionPlatform ExecutionPlatforms]] to use to execute jobs.
    * @param port The port to use for the HTTP daemon.
    * @param databaseConfig JDBC configuration for MySQL server
    * @param retryStrategy The strategy to use for execution retry. Default to exponential backoff.
    */
  def start(
    platforms: Seq[ExecutionPlatform] = CuttleProject.defaultPlatforms,
    port: Int = CuttleProject.port,
    databaseConfig: DatabaseConfig = CuttleProject.databaseConfig,
    retryStrategy: RetryStrategy = CuttleProject.retryStrategy
  ): Unit = {
    logger.info("Connecting to database")
    val transactor = Database.connect(databaseConfig)

    logger.info("Creating Executor")
    val executor = new Executor[S](platforms, transactor, logger, name, version)(retryStrategy)

    logger.info("Scheduler starting workflow")
    scheduler.start(jobs, executor, transactor, logger)

    logger.info("Starting server")
    Server.listen(port, onError = { e =>
      logger.error(e.getMessage)
      e.printStackTrace()
      InternalServerError(e.getMessage)
    })(routes)

    logger.info(s"Listening on http://localhost:$port")
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
  def apply[S <: Scheduling](name: String,
                             version: String = "",
                             description: String = "",
                             env: (String, Boolean) = ("", false),
                             authenticator: Auth.Authenticator = Auth.GuestAuth)(
    jobs: Workload[S])(implicit scheduler: Scheduler[S], logger: Logger): CuttleProject[S] = {
    val cronApp = CronApp()
    new CuttleProject(name, version, description, env, jobs, scheduler, cronApp.routes, logger)
  }

  private[CuttleProject] val defaultPlatforms: Seq[ExecutionPlatform] = {
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

  private[CuttleProject] val port = 8888
  private[CuttleProject] val databaseConfig = DatabaseConfig.fromEnv
  private[CuttleProject] val retryStrategy = RetryStrategy.SimpleRetryStategy(1.minute)
}
