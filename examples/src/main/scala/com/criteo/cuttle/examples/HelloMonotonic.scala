package com.criteo.cuttle.examples

import com.criteo.cuttle.{CuttleProject, Job, utils}
import com.criteo.cuttle.monotonic.{Monotonic, MonotonicScheduler}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration

object HelloMonotonic {

  def main(args: Array[String]) = {

    implicit val triggerFun = () => {
      utils.Timeout(Duration.create(1, "min")) map { _ => (Math.random() * 100).toInt }
    }

    implicit val scheduler = new MonotonicScheduler[Int]()

    // TODO: implement scheduling with max parallel parameter to block at job level
    val world = Job("world", scheduling = Monotonic()) { implicit exec =>
      Future(println(s"WORLD!!! exec param: ${exec.context.executionParams} || exec time: ${exec.context.executionTime}"))
    }

    val hello = Job("hello", scheduling = Monotonic()) { implicit exec =>
      Future(println("HELLO!!!!!"))
    }

    CuttleProject("Hello World") {
      world dependsOn hello
    }.start()
  }

}
