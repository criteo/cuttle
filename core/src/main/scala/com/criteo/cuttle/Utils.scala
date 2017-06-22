package com.criteo.cuttle

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.{Duration}

import java.util.{UUID}
import java.util.concurrent.{Executors, TimeUnit}

import lol.http.{PartialService, Service}

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

  /**
    * Allows chaining of method orFinally
    * from a PartialService that returns a
    * non-further-chainable Service.
    */
  implicit class PartialServiceConverter(val service: PartialService) extends AnyVal {
    def orFinally(finalService : Service) : Service = {
      service.orElse(toPartial(finalService))
    }

    private def toPartial(service : Service) : PartialService = {
      case e => service(e)
    }
  }
}
