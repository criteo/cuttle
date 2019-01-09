package com.criteo.cuttle.cron

import java.io.PrintWriter
import java.time.Instant

import scala.concurrent.Future

import org.scalatest.{FlatSpec, Matchers}

import com.criteo.cuttle.Auth.User
import com.criteo.cuttle.{Job, _}
import com.criteo.cuttle.ThreadPools.Implicits.sideEffectThreadPool

/**
  * These tests don't verify anything but the rendering for "/" and "/executions" pages.
  * It was used for dev purposes but could be useful on the CI as a first indicator that pages could be rendered w/out throwing.
  */
// TODO: Implement a snapshot testing of the UI ala https://jestjs.io/docs/en/snapshot-testing
class UISpec extends FlatSpec with Matchers {
  val tickerJob = Job(
    id = "ticker_job",
    scheduling = CronScheduling("0-59/10 * * ? * *", 1),
    name = "Ticker Job",
    description = "Get ticker for Bitcoin price from CoinMarketCap"
  ) { implicit e =>
    Future.successful(Completed)
  }
  val avgJob = Job(id = "avg_job",
                   name = "Average Job",
                   scheduling = CronScheduling("0-59/10 * * ? * *", 10),
                   description = "Average Bitcoin price for last 3 value") { implicit e =>
    Future.successful(Completed)
  }
  val workload = CronWorkload(Set(tickerJob, avgJob))
  implicit val scheduler = CronScheduler(logger)
  val project = CronProject(
    name = "Hello Cron Scheduling Example",
    version = "0.0.1",
    description = "My first Cron with Cuttle project"
  )(workload)
  implicit val xa = com.criteo.cuttle.Database.newHikariTransactor(DatabaseConfig.fromEnv).allocated.unsafeRunSync()._1
  val executor = new Executor[CronScheduling](Seq.empty, xa, logger, project.name, project.version)

  val ui = UI(project, executor)

  def saveToFile(s: String, fileName: String) = {
    // running project inside of a Cron module default directory is set as module root
    val writer = new PrintWriter(fileName)
    writer.print(s)
    writer.close()
  }

  val avgExecution = Execution[CronScheduling](
    "786d1b69-a603-4eb8-9178-fed2a195a1ed",
    avgJob,
    CronContext(Instant.now())(0),
    new ExecutionStreams {
      override private[cuttle] def writeln(str: CharSequence): Unit = ???
    },
    Seq.empty,
    "",
    ""
  )

  "home page" should "render active jobs" in {
    val activeJobs = Map(
      tickerJob -> Left(Instant.now()),
      avgJob -> Right(avgExecution)
    )
    val pausedJobs = Map(
      tickerJob -> PausedJob(tickerJob.id, User("Bobby"), Instant.now())
    )
    val activeAndPausedJobs = (activeJobs, pausedJobs)
    saveToFile(ui.home(activeAndPausedJobs).content, "src/test/resources/index.html")
  }

  "executions page" should "render execution list" in {
    val executionLogs = Seq(
      avgExecution.toExecutionLog(ExecutionStatus.ExecutionSuccessful)
    )
    saveToFile(ui.executions(avgJob, executionLogs).content, "src/test/resources/executions.html")
  }
}
