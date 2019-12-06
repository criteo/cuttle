// Example: Hello CRON scheduling!

// This a minimal cuttle project demonstrating the CRON scheduler
package com.criteo.cuttle.examples

// The main package contains everything needed to create
// a cuttle project.
import com.criteo.cuttle._
import com.criteo.cuttle.cron.{CronProject, CronScheduler, CronScheduling, CronWorkload}
import com.criteo.cuttle.cron.CronPipeline._

import scala.io.Source

// The local platform allows to locally fork some processes
// (_here we will just fork shell scripts_).
import com.criteo.cuttle.platforms.local._

import scala.concurrent.Future

object HelloCronScheduling {

  // A cuttle project is just embedded into any Scala application.
  def main(args: Array[String]): Unit = {
    // We are going to:
    // 1. Each 10 seconds call https://api.coinmarketcap.com to get the last available price of Bitcoin
    // and save this price to a file on file system.
    // 2. Each 10 seconds comptute the average of last 3 saved prices.
    // If we have less than 3 prices our job is going to fail.
    val fileName = "price.log"

    // __Now let's define our first cron job!__
    val tickerJob = Job(
      id = "ticker_job",
      // We define out schedule by a simple Cron expression, that is parsed with cron4s library.
      // For more documentation see https://github.com/alonsodomin/cron4s.
      // 1 corresponds to the maximum number of retries that we allow for this job
      scheduling = CronScheduling(1),
      name = "Ticker Job",
      description = "Get ticker for Bitcoin price from CoinMarketCap"
    ) {
      // The side effect function takes the execution as parameter. The execution
      // contains useful meta data as well as the __context__ which is basically the
      // input data for our execution.
      // In our case the context contains the scheduling date and the retry number.
      implicit e =>
        // We can read execution parameters from the context.
        val timestamp = e.context.instant

        // We can output the information in execution streams that are persisted
        // is a state DB.
        e.streams.info(s"Launching the job ${e.job.id} at $timestamp")

        // Now do some real work in BASH by calling CoinMarketCap API and processing the result with Python.
        exec"""
            bash -c 'curl https://api.coinmarketcap.com/v2/ticker/1/ | python -c "import sys, json; print(json.load(sys.stdin)[\"data\"][\"quotes\"][\"USD\"][\"price\"])" >> $fileName'
          """ ()
    }

    // __Let's compute the average of 3 last Bitcoin prices, if we have less than 3 entries this job will fail
    val avgJob = Job(id = "avg_job",
                     scheduling = CronScheduling(10),
                     description = "Average Bitcoin price for last 3 value") { implicit e =>
      Future {
        // We use plain old Scala APi to interact with file system.
        val lines = Source.fromFile(fileName).getLines.toList
        val last3Lines = lines.drop(lines.length - 3)
        if (last3Lines.length < 3)
          // Just throw an exception if you want to fail.
          throw new UnsupportedOperationException("We have less than 3 values to compute the average!")
        else {
          // We compute the average, it can fail in some locales.
          val avgPrice = last3Lines.map(_.toDouble).sum / 3
          // We output some ASCII art just to make our message visible in the logs :)
          e.streams.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
          e.streams.info(s"The average of last 3 Bitcoin prices is $avgPrice")
          e.streams.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
          Completed
        }
      }
    }

    // Jobs are grouped in workload.
    val workload = CronWorkload(Set(tickerJob.toCronDag("0-59/10 * * ? * *", "ticker_dag"),
    avgJob.toCronDag("0-59/10 * * ? * *", "avg_dag")))

    // Instantiate Cron scheduler with a default stdout logger which will be passed implicitly to Cron project.
    implicit val scheduler = CronScheduler(logger)

    // Project instantiation, it takes an implicit scheduler that we've just defined!
    val project = CronProject(
      name = "Hello Cron Scheduling Example",
      version = "0.0.1",
      description = "My first Cron with Cuttle project"
    )(workload)

    // __Finally start it!__
    project.start()
  }
}
