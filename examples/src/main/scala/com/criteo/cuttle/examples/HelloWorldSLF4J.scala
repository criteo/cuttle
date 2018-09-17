// Example: Hello cuttle SLF4J!

// This examples shows how we can redirect slf4J and log4J logs to the execution stream by registering a
// custom log appender that taps into the map of threads names to execution streams to redirect log events.
package com.criteo.cuttle.examples

import com.criteo.cuttle._
import com.criteo.cuttle.log.ExecutionStreamsAppender
import org.slf4j.LoggerFactory
import scala.concurrent.Future
import com.criteo.cuttle.timeseries._
import java.time.ZoneOffset.UTC
import java.time._

import scala.concurrent.duration._

object HelloWorldSLF4J {

  // enable Log4J lo redirection to execution streams
  ExecutionStreamsAppender.registerAsRootLogger()

  // A logger for this class
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val start: Instant = LocalDate.now.minusDays(7).atStartOfDay.toInstant(UTC)
    val hello1 =
      Job("hello1", hourly(start), "Hello 1", tags = Set(Tag("hello"))) {
        implicit e =>
          val partitionToCompute = e.context.start + "-" + e.context.end

          e.park(1.seconds) flatMap { _ =>
          (Future {
            log.info(s"A first future for Hello1")
            Thread.sleep(5000)
            log.info(s"Completed first future for Hello1")
          } zip Future {
            log.warn(s"A second, parallel future for Hello1")
            Thread.sleep(5000)
            log.warn(s"Completed second, parallel future for Hello1")
          }) flatMap (_ => Future {
            log.error(s"Final future for Hello1")
            Thread.sleep(5000)
            log.error(s"Completed final future for Hello1")
            Completed
          })
        }
      }

    // Finally we bootstrap our cuttle project.
    CuttleProject("Hello World", env = ("dev", false))(hello1).start()
  }
}
