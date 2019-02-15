// Test: Cuttle timeseries with many jobs!

// This a very simple cuttle project using the time series scheduler
// to execute a lot of jobs to do load tests
package com.criteo.cuttle.examples

import com.criteo.cuttle._
import com.criteo.cuttle.timeseries._
import java.time.ZoneOffset.UTC
import java.time._

import scala.concurrent.Future
import scala.concurrent.duration._

object TestTimeSeriesWithManyJobs {

  def main(args: Array[String]): Unit = {

    val start: Instant = LocalDate.now.atStartOfDay.toInstant(UTC)

    val jobs: Workflow = (1 to 1500).toArray
      .map({ i =>
        Job(s"hello-with-a-relatively-long-id-just-for-the-fun-to-break-things$i",
            daily(UTC, start),
            s"Hello $i",
            tags = Set(Tag("hello"), Tag(s"hello-${i / 100}xx"))) { implicit e =>
          val partitionToCompute = e.context.start + "-" + e.context.end
          e.streams.info(s"Hello $i for $partitionToCompute")
          Future.successful(Completed)
        }
      })
      .foldLeft(Workflow.empty)(_ and _)

    val world: Job[TimeSeries] = Job("world", daily(UTC, start), "World", tags = Set(Tag("world"))) { implicit e =>
      e.streams.info("World!")
      e.park(1.seconds).map(_ => Completed)
    }

    CuttleProject("Hello World", version = "123", env = ("dev", false)) {
      world dependsOn jobs
    }.start(logsRetention = Some(1.minute))
  }
}
