package org.criteo.langoustine.examples

import org.criteo.langoustine._
import timeserie._

object HelloWorld {

  def main(args: Array[String]): Unit = {

    val hello = Job("hello", hourly())
    val world = Job("world", hourly())

    Langoustine.run(
      world dependsOn hello,
      httpPort = args.lift(0).map(_.toInt).getOrElse(8888))
  }

}