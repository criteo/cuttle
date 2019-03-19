// Example: Hello cuttle timeseries!

// This a very simple cuttle project using the time series scheduler
// to execute a bunch of shell scripts
package com.criteo.cuttle.examples

// The main package contains everything needed to create
// a cuttle project.
import com.criteo.cuttle._
import cats.implicits._

// The local platform allows to locally fork some processes
// (_here we will just fork shell scripts_).
import com.criteo.cuttle.platforms.local._

// We will use the time series scheduler for this project.
import com.criteo.cuttle.timeseries._

// We also have to import the Java 8 time API, used by the
// time series scheduler.
import java.time.ZoneOffset.UTC
import java.time._

import scala.concurrent.duration._
import scala.concurrent._

object HelloTimeSeries {

  // A cuttle project is just embeded into any Scala application.
  def main(args: Array[String]): Unit = {

    // We define a common start date for all our jobs. This is required by the
    // time series scheduler to define a start date for each job. Here we dynaically
    // compute it as 7 days ago (_and we round it to midnight UTC_).
    val start: Instant = LocalDate.now.minusDays(7).atStartOfDay.toInstant(UTC)

    // Here is our first job. The second parameter is the scheduling configuration.
    // __hello1__ is defined as a job computing hourly partitions starting at the start
    // date declared before.
    val hello1 =
      Job("hello1", hourly(start), "Hello 1", tags = Set(Tag("hello"))) {
        implicit e =>
          def sleep(n: Int): Future[Unit] = n match {
            case 0 => Future.successful(())
            case x@_ =>
              e.streams.info("sleeping...")
              e.park(1.seconds) >> sleep(n - 1)
          }

          sleep(200).map(_ => Completed)
      }

    val hello2 =
      Job("hello2", hourly(start), "Hello 2", tags = Set(Tag("hello"))) {
        implicit e =>
          def sleepBlockingExecContext(n: Int): Future[Unit] = Future {
            java.lang.Thread.sleep(n * 1000)
            ()
          }

          sleepBlockingExecContext(200).map(_ => Completed)
      }

    val hello3 =
      Job("hello3", hourly(start), "Hello 3", tags = Set(Tag("hello"))) {
        implicit e =>
          def sleepBlockingMainContext(n: Int): Future[Unit] = {
            (1 to n).map { _ =>
               e.streams.info("Sleeping 1s...")
               java.lang.Thread.sleep(1000)
            }

            Future.successful(())
          }


          sleepBlockingMainContext(200).map(_ => Completed)
      }

    // Finally we bootstrap our cuttle project.
    CuttleProject("Hello World", version = "123", env = ("dev", false)) {
      // Any cuttle project contains a Workflow to execute. This Workflow is composed from jobs
      // or from others smaller Workflows.
      hello1 and hello2 and hello3
    }.
    // Starts the scheduler and an HTTP server.
    start(logsRetention = Some(1.hour))
  }
}
