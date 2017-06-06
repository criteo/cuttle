package com.criteo.cuttle.examples

import com.criteo.cuttle._
import timeseries._
import java.io._
import java.time.ZoneOffset.UTC
import java.time._

import scala.concurrent.ExecutionContext.Implicits.global

object HelloWorld {

  def main(args: Array[String]): Unit = {
    // 7 days ago at 00:00 UTC
    val start: Instant = LocalDate.now.minusDays(7).atStartOfDay.toInstant(UTC)

    val hello1 =
      Job("hello1", hourly(start)) { implicit e =>
        sh"""
          echo "Hello 1"
          sleep 1
        """.exec()
      }

    val hello2 =
      Job("hello2", hourly(start)) { implicit e =>
        sh"""
          echo "Looping for 20 seconds..."
          for i in {1..20}; do
            date
            sleep 1
          done
          echo "Ok"
        """.exec()
      }

    val hello3 =
      Job("hello3", hourly(start)) { implicit e =>
        sh"""
          echo "Hello 3"
          sleep 3
        """.exec().map { _ =>
          // Artificially fail for Yesterday 00 to 01
          // if /tmp/hello3_success does not exist
          if (e.context.start == LocalDate.now.minusDays(1).atStartOfDay.toInstant(UTC)
              && !new File("/tmp/hello3_success").exists) {
            e.streams.error("Oops, please create the /tmp/hello3_success file to make this execution pass...")
            sys.error("Oops!!!")
          }
        }
      }

    val world = Job("world", daily(start, UTC)) { implicit e =>
      sh"""
        echo "World"
        sleep 6
      """.exec()
    }

    Cuttle("Hello World") {
      world dependsOn (hello1 and hello2 and hello3)
    }.run()
  }

}
