package com.criteo.cuttle.examples

import com.criteo.cuttle._
import timeseries._
import java.io._
import java.time.ZoneOffset.UTC
import java.time._

import scala.concurrent.ExecutionContext.Implicits.global

object HelloWorld {

  def main(args: Array[String]): Unit = {
    // Yesterday at 00:00 UTC
    val start: Instant = LocalDate.now.minusDays(1).atStartOfDay.toInstant(UTC)

    val hello1 =
      Job("hello1", hourly(start)) { implicit e =>
        sh"""
          echo "Hello 1"
          sleep 10
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
          sleep 30
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

    val hello4 =
      Job("hello4", hourly(start)) { implicit e =>
        sh"""
          echo "Hello 4"
          sleep 10
        """.exec()
      }

    val hello5 =
      Job("hello5", hourly(start)) { implicit e =>
        sh"""
          echo "Hello 5"
          sleep 10
        """.exec()
      }

    val hello6 =
      Job("hello6", hourly(start)) { implicit e =>
        sh"""
          echo "Hello 6"
          sleep 10
        """.exec()
      }

    val hello7 =
      Job("hello7", hourly(start)) { implicit e =>
        sh"""
          echo "Hello 7"
          sleep 10
        """.exec()
      }
    val hello8 =
      Job("hello8", hourly(start)) { implicit e =>
        sh"""
          echo "Hello 8"
          sleep 10
        """.exec()
      }
    val hello9 =
      Job("hello9", hourly(start)) { implicit e =>
        sh"""
          echo "Hello 9"
          sleep 10
        """.exec()
      }
    val hello10 =
      Job("hello10", hourly(start)) { implicit e =>
        sh"""
          echo "Hello 10"
          sleep 10
        """.exec()
      }

    val world1 = Job("world1", daily(start, UTC)) { implicit e =>
      sh"""
        echo "World 1"
        sleep 1
      """.exec()
    }

    val world2 = Job("world2", daily(start, UTC)) { implicit e =>
      sh"""
        echo "World 2"
        sleep 1
      """.exec()
    }

    val world3 = Job("world3", daily(start, UTC)) { implicit e =>
      sh"""
        echo "World 3"
        sleep 1
      """.exec()
    }

    val langoustine = Job("langoustine", daily(start, UTC)) { implicit e =>
      sh"""
        echo "Langoustine"
        sleep 1
      """.exec()
    }

    val cuttle = Job("cuttle", daily(start, UTC)) { implicit e =>
      sh"""
        echo "Cuttle"
        sleep 1
      """.exec()
    }

    val scheduler = Job("scheduler", daily(start, UTC)) { implicit e =>
      sh"""
        echo "Scheduler"
        sleep 60
      """.exec()
    }

    Cuttle("Hello World") {
      (langoustine and cuttle and scheduler) dependsOn ((world1 dependsOn (hello2 and hello4 and hello6 and hello7 and hello8 and hello10)) and (world2 dependsOn (hello1 and hello3 and hello5 and hello7 and hello9)) and (world3 dependsOn (hello7 and hello8 and hello9 and hello10)))
    }.run()
  }

}
