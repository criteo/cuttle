package com.criteo.cuttle.examples

import com.criteo.cuttle._
import timeseries._

import java.io._
import java.time._
import java.time.temporal._

import scala.concurrent.ExecutionContext.Implicits.global

object HelloWorld {

  def main(args: Array[String]): Unit = {
    // Yesterday at 00:00 UTC
    val start = LocalDateTime.now.minusDays(1).truncatedTo(ChronoUnit.DAYS)

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
          echo "Hello 2"
          sleep 2
        """.exec()
      }

    val hello3 =
      Job("hello3", hourly(start)) { implicit e =>
        sh"""
          echo "Hello 3"
          sleep 3
        """.exec().map { _ =>
          // Artificially fail for the first hour of the computation period
          // if /tmp/hello3_success does not exist
          if (e.context.start == start && !new File("/tmp/hello3_success").exists) {
            e.streams.error("Oops, please create the /tmp/hello3_success file to make this execution pass...")
            sys.error("Oops!!!")
          } else {
            e.streams.info("Success file found!")
          }
        }
      }

    val world = Job("world", daily("UTC", start)) { implicit e =>
      sh"""
        echo "World"
        sleep 1
      """.exec()
    }

    Cuttle("Hello World") {
      world dependsOn (hello1 and hello2 and hello3)
    }.run()
  }

}
