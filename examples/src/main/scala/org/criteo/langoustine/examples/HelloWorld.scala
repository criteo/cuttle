package org.criteo.langoustine.examples

import org.criteo.langoustine._
import timeseries._

object HelloWorld {

  def main(args: Array[String]): Unit = {
    val start = date"2017-01-01T00:00Z"

    val hello = Job("hello", hourly(start))
    val world = Job("world", hourly(start))

    Langoustine.run(
      world dependsOn hello,
      httpPort = args.lift(0).map(_.toInt).getOrElse(8888))
  }

}
