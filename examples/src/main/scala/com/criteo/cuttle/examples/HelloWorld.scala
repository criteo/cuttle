package com.criteo.cuttle.examples

import com.criteo.cuttle._
import com.criteo.cuttle.logging.default._
import platforms.local._
import timeseries._

import java.time._
import java.time.ZoneOffset.{UTC}

object HelloWorld {

  def main(args: Array[String]): Unit = {
    // 7 days ago at 00:00 UTC
    val start: Instant = LocalDate.now.minusDays(7).atStartOfDay.toInstant(UTC)

    val hello1 =
      Job("hello1", hourly(start), "Hello 1") { implicit e =>
        sh"""
          echo "Hello 1"
          echo check my project page https://github.com/criteo/cuttle
          sleep 1
        """.exec()
      }

    val hello2 =
      Job("hello2", hourly(start), "Hello 2") { implicit e =>
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
      Job("hello3", hourly(start), "Hello 3", tags = Set(Tag("unsafe job"))) { implicit e =>
        sh"""
          echo "Hello 3"
          sleep 3
        """.exec().map { _ =>
          // Artificially fail for 2 days ago 00 to 01
          // if /tmp/hello3_success does not exist
          if (e.context.start == LocalDate.now.minusDays(2).atStartOfDay.toInstant(UTC)
              && !new java.io.File("/tmp/hello3_success").exists) {
            e.streams.error("Oops, please create the /tmp/hello3_success file to make this execution pass...")
            sys.error("Oops!!!")
          }
          else {
            Completed
          }
        }
      }

    val world = Job("world", daily(start, UTC), "World") { implicit e =>
      sh"""
        echo "World"
        sleep 6
      """.exec()
    }

    CuttleProject("Hello World", env = ("Demo", false)) {
      world dependsOn (hello1 and hello2 and hello3)
    }.start()
  }
}
