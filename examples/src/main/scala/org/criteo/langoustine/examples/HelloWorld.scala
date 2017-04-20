package org.criteo.langoustine.examples

import org.criteo.langoustine._
import timeseries._

object HelloWorld {

  def main(args: Array[String]): Unit = {
    val start = date"2017-04-06T00:00Z"

    val tagPolite = Tag("Polite", Some("Jobs that greet you politely"))
    val tagBigFilter = Tag(name = "BigFilter")

    val hello = Job("0", Some("Hello"), Some("The Hello Job"), Set(tagPolite), hourly(start)) { implicit e =>
      sh"""
        echo "${e.context} -> Hello";
        sleep 3
      """.exec()
    }

    val world = Job("1", Some("World"), Some("The World Job"), Set(tagBigFilter), daily("UTC", start)) { implicit e =>
      sh"""
        echo "${e.context} -> World";
        sleep 1
      """.exec()
    }

    Langoustine("Hello World") {
      world dependsOn hello
    }.run()
  }

}
