package com.criteo.cuttle.timeseries

import java.time.ZoneOffset.UTC
import java.time.temporal.ChronoUnit.HOURS
import java.time.{Duration, Instant, LocalDate}
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration.Inf
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

import com.wix.mysql._
import com.wix.mysql.config.Charset._
import com.wix.mysql.config._
import com.wix.mysql.distribution.Version._

import com.criteo.cuttle.platforms.local._
import com.criteo.cuttle.timeseries.TimeSeriesUtils.{Run, TimeSeriesJob}
import com.criteo.cuttle.{Auth, Database => CuttleDatabase, _}

object TimeSeriesSchedulerIntegrationTests {
  // TODO: turn this into a unit test. This is not done for now as the thread pool responsible for checking the lock on
  // the state database creates non-daemon threads, which would result in the unit test not ending unless it is interrupted
  // from the outside.
  def main(args: Array[String]): Unit = {
    val config = {
      MysqldConfig
        .aMysqldConfig(v5_7_latest)
        .withCharset(UTF8)
        .withTimeout(3600, TimeUnit.SECONDS)
        .withPort(3388)
        .build()
    }
    val mysqld = EmbeddedMysql.anEmbeddedMysql(config).addSchema("cuttle_dev").start()

    println("started!")
    println("if needed you can connect to this running db using:")
    println("> mysql -u root -h 127.0.0.1 -P 3388 cuttle_dev")

    val project = CuttleProject("Hello World", version = "123", env = ("dev", false)) {
      Jobs.childJob dependsOn Jobs.rootJob
    }

    val retryImmediatelyStrategy = new RetryStrategy {
      def apply[S <: Scheduling](job: Job[S], context: S#Context, previouslyFailing: List[String]) = Duration.ZERO
      def retryWindow = Duration.ZERO
    }

    val xa = CuttleDatabase.connect(DatabaseConfig(Seq(DBLocation("127.0.0.1", 3388)), "cuttle_dev", "root", ""))
    val executor =
      new Executor[TimeSeries](Seq(LocalPlatform(maxForkedProcesses = 10)), xa, logger, project.name, project.version)(
        retryImmediatelyStrategy)
    val scheduler = project.scheduler

    scheduler.initialize(project.jobs, xa, logger)

    var runningExecutions = Set.empty[Run]

    logger.info("Starting 'root-job' and 'child-job'")
    runningExecutions = doSynchronousExecutionStep(scheduler, runningExecutions, project.jobs, executor, xa)
    assert(runningExecutionsToJobIdAndResult(runningExecutions).equals(Set(("root-job", true))))

    logger.info("'root-job' completed, the child job 'child-job' is triggered but fails")
    runningExecutions = doSynchronousExecutionStep(scheduler, runningExecutions, project.jobs, executor, xa)
    assert(runningExecutionsToJobIdAndResult(runningExecutions).equals(Set(("child-job", false))))

    logger.info("'child-job' is paused")
    val guestUser = Auth.User("Guest")
    scheduler.pauseJobs(Set(Jobs.childJob), executor, xa)(guestUser)
    assert(
      (scheduler
        .pausedJobs()
        .map { case PausedJob(jobId, user, date) => (jobId, user) })
        .equals(Set(("child-job", guestUser))))
    runningExecutions = doSynchronousExecutionStep(scheduler, runningExecutions, project.jobs, executor, xa)
    assert(runningExecutions.isEmpty)

    logger.info("'child-job' is resumed")
    scheduler.resumeJobs(Set(Jobs.childJob), xa)(guestUser)
    assert(scheduler.pausedJobs().isEmpty)
    runningExecutions = doSynchronousExecutionStep(scheduler, runningExecutions, project.jobs, executor, xa)
    assert(runningExecutionsToJobIdAndResult(runningExecutions).equals(Set(("child-job", true))))

    logger.info("No more jobs to schedule for now")
    runningExecutions = doSynchronousExecutionStep(scheduler, runningExecutions, project.jobs, executor, xa)
    assert(runningExecutions.isEmpty)

    mysqld.stop()
  }

  private def doSynchronousExecutionStep(scheduler: TimeSeriesScheduler,
                                         runningExecutions: Set[Run],
                                         workflow: Workflow,
                                         executor: Executor[TimeSeries],
                                         xa: XA): Set[(TimeSeriesJob, TimeSeriesContext, Future[Completed])] = {
    val newRunningExecutions =
      scheduler.updateCurrentExecutionsAndSubmitNewExecutions(runningExecutions, workflow, executor, xa)

    import scala.concurrent.ExecutionContext.Implicits.global
    val executionsResult = Future.sequence(newRunningExecutions.map {
      case (job, executionContext, futureResult) =>
        logger.info(s"Executed ${job.id}")
        futureResult
    })
    Await.ready(executionsResult, Inf)
    newRunningExecutions
  }

  private def runningExecutionsToJobIdAndResult(runningExecutions: Set[Run]): Set[(String, Boolean)] = {
    def executionResultToBoolean(result: Future[Completed]) = result.value match {
      case Some(Success(_)) => true
      case Some(Failure(t)) => false
      case None             => throw new Exception("The execution should have completed.")
    }
    runningExecutions.map {
      case (job, executionContext, futureResult) => (job.id, executionResultToBoolean(futureResult))
    }
  }

  object Jobs {
    private val start: Instant = LocalDate.now.minusDays(1).atStartOfDay.toInstant(UTC)

    val rootJob = Job("root-job", daily(UTC, start)) { implicit e =>
      Future(Completed)
    }

    private var failedOnce = false
    // A job which fails the first time it runs
    val childJob = Job("child-job", daily(UTC, start)) { implicit e =>
      if (!failedOnce) {
        failedOnce = true
        throw new Exception("Always fails at the first execution")
      } else {
        Future(Completed)
      }
    }
  }
}
