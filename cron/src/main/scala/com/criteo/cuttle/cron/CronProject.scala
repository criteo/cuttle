package com.criteo.cuttle.cron

import com.criteo.cuttle.Auth.Authenticator
import com.criteo.cuttle.ThreadPools.Implicits.serverThreadPool
import com.criteo.cuttle.ThreadPools._
import com.criteo.cuttle._
import doobie.implicits._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import lol.http._

import scala.concurrent.duration._

/**
  * A cuttle project is a workflow to execute with the appropriate scheduler.
  * See the [[CronProject]] companion object to create projects.
  */
class CronProject private[cuttle] (val name: String,
                                   val version: String,
                                   val description: String,
                                   val env: (String, Boolean),
                                   val workload: CronWorkload,
                                   val scheduler: CronScheduler,
                                   val authenticator: Authenticator,
                                   val logger: Logger) {

  /**
    * Start scheduling and execution with the given environment. It also starts
    * an HTTP server providing an Web UI and a JSON API.
    *
    * @param platforms      The configured [[ExecutionPlatform ExecutionPlatforms]] to use to execute jobs.
    * @param port           The port to use for the HTTP daemon.
    * @param databaseConfig JDBC configuration for MySQL server
    * @param retryStrategy  The strategy to use for execution retry. Default to exponential backoff.
    * @param logsRetention  If specified, automatically clean the execution logs older than the given duration.
    */
  def start(
    platforms: Seq[ExecutionPlatform] = CronProject.defaultPlatforms,
    port: Int = CronProject.port,
    databaseConfig: DatabaseConfig = CronProject.databaseConfig,
    retryStrategy: RetryStrategy = CronProject.retryStrategy,
    logsRetention: Option[Duration] = None
  ): Unit = {
    logger.info("Connecting to database")
    implicit val transactor = com.criteo.cuttle.Database.connect(databaseConfig)(logger)

    logger.info("Applying migrations to database")
    Database.doSchemaUpdates.transact(transactor).unsafeRunSync
    logger.info("Database up-to-date")

    logger.info("Creating Executor")
    val executor =
      new Executor[CronScheduling](platforms, transactor, logger, name, version, logsRetention)(retryStrategy)

    logger.info("Scheduler starting workflow")
    scheduler.start(workload, executor, transactor, logger)

    logger.info("Creating Cron App for http server")
    val cronApp = CronApp(this, executor)

    logger.info("Starting server")
    Server.listen(port, onError = { e =>
      logger.error(e.getMessage)
      e.printStackTrace()
      InternalServerError(e.getMessage)
    })(cronApp.routes)

    logger.info(s"Listening on http://localhost:$port")
  }

}

/**
  * Create new projects using a timeseries scheduler.
  */
object CronProject {

  /**
    * Create a new project.
    *
    * @param name          The project name as displayed in the UI.
    * @param version       The project version as displayed in the UI.
    * @param description   The project version as displayed in the UI.
    * @param env           The environment as displayed in the UI (The string is the name while the boolean indicates
    *                      if the environment is a production one).
    * @param authenticator The way to authenticate HTTP request for the UI and the private API.
    * @param jobs          The workflow to run in this project.
    * @param scheduler     The scheduler instance to use to schedule the Workflow jobs.
    * @param logger        The logger to use to log internal debug informations.
    */
  def apply(
    name: String,
    version: String = "",
    description: String = "",
    env: (String, Boolean) = ("", false),
    authenticator: Auth.Authenticator = Auth.GuestAuth
  )(jobs: CronWorkload)(implicit scheduler: CronScheduler, logger: Logger): CronProject =
    new CronProject(name, version, description, env, jobs, scheduler, authenticator, logger)

  private[CronProject] val defaultPlatforms: Seq[ExecutionPlatform] = {
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

  private[CronProject] lazy val port = 8888
  private[CronProject] lazy val databaseConfig = DatabaseConfig.fromEnv
  private[CronProject] lazy val retryStrategy = RetryStrategy.SimpleRetryStategy(1.minute)

  implicit def projectEncoder = new Encoder[CronProject] {
    override def apply(project: CronProject) =
      Json.obj(
        "name" -> project.name.asJson,
        "version" -> Option(project.version).filterNot(_.isEmpty).asJson,
        "description" -> Option(project.description).filterNot(_.isEmpty).asJson,
        "scheduler" -> project.scheduler.name.asJson,
        "env" -> Json.obj(
          "name" -> Option(project.env._1).filterNot(_.isEmpty).asJson,
          "critical" -> project.env._2.asJson
        )
      )
  }
}
