package com.criteo.cuttle

import java.util.UUID
import java.util.concurrent.{Executors, ThreadFactory, TimeUnit}

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, Promise}

import lol.http.{PartialService, Service}


package object utils {

  private[cuttle] object Timeout {
    private val scheduler = Executors.newScheduledThreadPool(1,
      new ThreadFactory() {
        def newThread(r: Runnable): Thread = {
          val t = Executors.defaultThreadFactory.newThread(r)
          t.setDaemon(true)
          t
        }
      })

    def apply(timeout: Duration): Future[Unit] = {
      val p = Promise[Unit]()
      scheduler.schedule(
        new Runnable { def run(): Unit = p.success(()) },
        timeout.toMillis,
        TimeUnit.MILLISECONDS
      )
      p.future
    }
  }

  private[cuttle] object ExecuteAfter {
    def apply[T](delay: Duration)(block: => Future[T])(implicit executionContext: ExecutionContext) = {
      Timeout(delay).flatMap(_ => block)(executionContext)
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
    def orFinally(finalService: Service): Service =
      service.orElse(toPartial(finalService))

    private def toPartial(service: Service): PartialService = {
      case e => service(e)
    }
  }
}
