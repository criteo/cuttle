package com.criteo.cuttle

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.{Duration}

import java.util.{UUID}
import java.util.concurrent.{Executors, TimeUnit}

package object utils {

  private[cuttle] object Timeout {
    private val scheduler = Executors.newScheduledThreadPool(1)
    def apply(timeout: Duration): Future[Unit] = {
      val p = Promise[Unit]()
      scheduler.schedule(new Runnable { def run = p.success(()) }, timeout.toMillis, TimeUnit.MILLISECONDS)
      p.future
    }
  }

  private[cuttle] val never = Promise[Nothing]().future

  private[cuttle] def randomUUID(): String = UUID.randomUUID().toString

}
