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
  val authenticator: Authenticator,
  val logger: Logger
) {
  def start(
    platforms: Seq[ExecutionPlatform] = CuttleProject.defaultPlatforms,
    httpPort: Int = 8888,
    databaseConfig: DatabaseConfig = DatabaseConfig.fromEnv,
    retryStrategy: RetryStrategy = RetryStrategy.ExponentialBackoffRetryStrategy
  ): Unit = {
    val xa = Database.connect(databaseConfig)
    val executor = new Executor[S](platforms, xa, logger = logger)(retryStrategy)

    // Basic metric definitions
    val allJobs = workflow.vertices.map(_.id)
    metricRepository
      .addMetric(new GenericMetric {
        override def toString: String = {
          val (running, waiting) = executor.runningExecutionsSizes(allJobs)
          s"""scheduler_stat{type="running"} $running\nscheduler_stat{type="waiting"} $waiting"""
        }
      })
      .addMetric(Comment("Count as failing all jobs that have failed and are not running (throttledState) " +
        "and all jobs that have recently failed and are now running."))
      .addMetric(Gauge("scheduler_stat_count", () => executor.pausedExecutionsSize(allJobs), Seq("type" -> "paused")))
      .addMetric(Gauge("scheduler_stat_count", () => executor.failingExecutionsSize(allJobs), Seq("type" -> "failing")))
      .addMetric(
        Gauge("scheduler_stat_count", () => executor.archivedExecutionsSize(allJobs), Seq("type" -> "finished")))
      .addMetric(Gauge("scheduler_stat_count",
                       () => scheduler.getStats(allJobs).getOrElse("backfills", -1),
                       Seq("type" -> "backfills")))

    Server.listen(port = httpPort, onError = { e =>
      e.printStackTrace()
      InternalServerError(e.getMessage)
    })(App(this, executor, xa, logger, metricRepository).routes)
    logger.info(s"Listening on http://localhost:$httpPort")
    scheduler.start(workflow, executor, xa, logger)
  }
}

object CuttleProject {
  def apply[S <: Scheduling](name: String,
                             version: String = "",
                             description: String = "",
                             env: (String, Boolean) = ("", false),
                             authenticator: Authenticator = GuestAuth)(workflow: Workflow[S])(
    implicit scheduler: Scheduler[S],
    logger: Logger,
    metricRepository: MetricRepository): CuttleProject[S] =
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
