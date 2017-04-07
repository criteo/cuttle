package org.criteo.langoustine.examples

import org.criteo.langoustine._
import timeseries._

object HelloWorld {

  def main(args: Array[String]): Unit = {
    val start = date"2017-04-06T00:00Z"

    val hello = Job("hello", hourly(start)) { implicit e =>
      sh"""
        echo "${e.context} -> Hello";
        sleep 3
      """.exec()
    }
    val world = Job("world", daily("UTC", start)) { implicit e =>
      sh"""
        echo "${e.context} -> World";
        sleep 1
      """.exec()
    }

    Langoustine(world dependsOn hello).run()
  }

}
