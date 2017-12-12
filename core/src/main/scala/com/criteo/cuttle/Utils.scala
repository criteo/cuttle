package com.criteo.cuttle

import java.util.UUID
import java.util.concurrent.{Executors, ThreadFactory, TimeUnit}
import java.lang.management.ManagementFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, Promise}

import lol.http.{PartialService, Service}

/** A set of basic utilities useful to write workflows. */
package object utils {

  /** Creates a  [[scala.concurrent.Future Future]] that resolve automatically
    * after the given duration.
    */
  object Timeout {
    private val scheduler = Executors.newScheduledThreadPool(1, createDaemonThreadFactory())

    /** Creates a  [[scala.concurrent.Future]] that resolve automatically
      * after the given duration.
      *
      * @param duration Duration for the timeout.
      */
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
    def apply[T](delay: Duration)(block: => Future[T])(implicit executionContext: ExecutionContext) =
      Timeout(delay).flatMap(_ => block)(executionContext)
  }

  private[cuttle] val never = Promise[Nothing]().future

  private[cuttle] def randomUUID(): String = UUID.randomUUID().toString

  /**
    * Allows chaining of method orFinally
    * from a PartialService that returns a
    * non-further-chainable Service.
    */
  implicit private[cuttle] class PartialServiceConverter(val service: PartialService) extends AnyVal {
    def orFinally(finalService: Service): Service =
      service.orElse(toPartial(finalService))

    private def toPartial(service: Service): PartialService = {
      case e => service(e)
    }
  }

  private[cuttle] def getJVMUptime = ManagementFactory.getRuntimeMXBean.getUptime / 1000

  private[cuttle] def createDaemonThreadFactory(): ThreadFactory {
    def newThread(r: Runnable): Thread
  } = new ThreadFactory() {
    def newThread(r: Runnable): Thread = {
      val t = Executors.defaultThreadFactory.newThread(r)
      t.setDaemon(true)
      t
    }
  }
}
