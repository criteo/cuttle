// Example: Hello cuttle timeseries!

// This a very simple cuttle project using the time series scheduler
// to execute a bunch of shell scripts
package com.criteo.cuttle.examples

// The main package contains everything needed to create
// a cuttle project.
import com.criteo.cuttle._

// The local platform allows to locally fork some processes
// (_here we will just fork shell scripts_).
import com.criteo.cuttle.platforms.local._

// We will use the time series scheduler for this project.
import com.criteo.cuttle.timeseries._

// We also have to import the Java 8 time API, used by the
// time series scheduler.
import java.time.ZoneOffset.UTC
import java.time._
import java.time.{Duration => JavaDuration}

import scala.concurrent.duration._

object HelloTimeSeries {

  // A cuttle project is just embeded into any Scala application.
  def main(args: Array[String]): Unit = {

    // We define a common start date for all our jobs. This is required by the
    // time series scheduler to define a start date for each job. Here we dynamically
    // compute it as 7 days ago (_and we round it to midnight UTC_).
    val start: Instant = LocalDate.now.minusDays(1).atStartOfDay.toInstant(UTC)

    // Here is our first job. The second parameter is the scheduling configuration.
    // __hello1__ is defined as a job computing hourly partitions starting at the start
    // date declared before.
    val hello1 =
      Job("hello1", hourly(start), "Hello 1", tags = Set(Tag("hello"))) {
        // The side effect function takes the execution as parameter. The execution
        // contains useful meta data as well as the __context__ which is basically the
        // input data for our execution.
        implicit e =>
          // Because this job uses a time series scheduling configuration the context
          // contains the information about the time partition to compute, ie the start
          // and end date.
          val partitionToCompute = (e.context.start) + "-" + (e.context.end)

          e.streams.info(s"Hello 1 for $partitionToCompute")
          e.streams.info("Check my project page at https://github.com/criteo/cuttle")
          e.streams.info("Do it quickly! I will wait you here for 1 second")
          e.park(1.seconds).map(_ => Completed)
      }

    // Our second job is also on hourly job that executes a sh script.
    // The `exec` interpolation is provided by the local platform.
    // It allows us to declare a sh script to execute.
    // More details are in [[exec]] doc.
    val hello2 = Job("hello2", hourly(start), "Dependency for cuttle_example.world_stats") { implicit e =>
      exec"""sh -c '
         |    echo Looping for 20 seconds...
         |    for i in `seq 1 20`
         |    do
         |        date
         |        sleep 1
         |    done
         |    echo Ok
         |'""" ()
    }

    // Here is our third job. Look how we can also define some metadata such as a human friendly
    // name and a set of tags. This information is used in the UI to help retrieving your jobs.
    // This job will be executed in batching mode, it means that it will always wait for some period of
    // time(10 seconds here) and create a single executions for each 5 consequent partition available to compute.
    val hello3 =
      Job(
        "hello3",
        hourly(start, batching = TimeSeriesBatching(5, JavaDuration.ofSeconds(10))),
        "prepare-export-job.cuttle_example.hello3_stats_daily",
        tags = Set(Tag("hello"), Tag("unsafe"))
      ) { implicit e =>
        // Here we mix a Scala code execution and a sh script execution.
        e.streams.info("Hello 3 from an unsafe job")
        e.streams.info(s"My previous failures are ${e.previousFailures}")
        val completed = exec"sleep 3" ()

        completed.map { _ =>
          // We generate an artificial failure if the partition is for 2 days ago between 00 and 01
          // and if the `/tmp/hello3_success` file does not exist.
          if (e.context.start == LocalDate.now.minusDays(2).atStartOfDay.toInstant(UTC)
              && !new java.io.File("/tmp/hello3_success").exists) {
            e.streams.error("Oops, please create the /tmp/hello3_success file to make this execution pass...")

            // Throwing an exception is enough to fail the execution, but you can also return
            // a failed Future.
            sys.error("Oops!!!")
          } else {

            // The completed value is returned to cuttle to announce the job execution as
            // successful. In this case the time series scheduler will mark the partition as
            // successful for job __hello3__ and store this information in his internal state.
            Completed
          }
        }
      }

    // Our last job is a daily job. For the daily job we still need to announce a start date, plus
    // we need to define the time zone for which _days_ must be considered. The partitions for
    // daily jobs will usually be 24 hours, unless you are choosing a time zone with light saving.
    val world = Job("world", daily(UTC, start), "export-job.cuttle.world_stats", tags = Set(Tag("world"))) {
      implicit e =>
        e.streams.info("World!")
        // Here we compose our executions in a for-comprehension.
        for {
          _ <- e.park(3.seconds)
          completed <- exec"sleep 3" ()
        } yield completed
    }

    // Finally we bootstrap our cuttle project.
    CuttleProject("Hello World", version = "123", env = ("dev", false)) {
      // Any cuttle project contains a Workflow to execute. This Workflow is composed from jobs
      // or from others smaller Workflows.
      world dependsOn (hello1 and hello2 and hello3)
    }.
    // Starts the scheduler and an HTTP server.
    start(logsRetention = Some(1.hour))
  }
}
