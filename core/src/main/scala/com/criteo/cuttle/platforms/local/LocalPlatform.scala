package com.criteo.cuttle.platforms.local

import java.nio.ByteBuffer

import com.zaxxer.nuprocess._
import java.util.UUID

import scala.concurrent.{Future, Promise}
import com.criteo.cuttle._
import com.criteo.cuttle.platforms.ExecutionPool
import lol.http.PartialService

case class LocalPlatform[S <: Scheduling](maxForkedProcesses: Int)(implicit contextOrdering: Ordering[S#Context])
  extends ExecutionPlatform[S] {

  private val pool = new ExecutionPool(name = "local", concurrencyLimit = maxForkedProcesses)

  override def waiting: Set[Execution[S]] = pool.waiting

  override lazy val routes: PartialService = pool.routes

  def runInPool(e: Execution[S])(f: () => Future[Unit]): Future[Unit] = pool.runInPool(e)(f)
}

object LocalPlatform {
  def fork(command: String) = {
    val script = command.stripMargin('|')
    new LocalProcess(new NuProcessBuilder("sh", "-ce", script)) {
      override def toString = script
    }
  }
}

class LocalProcess(private val process: NuProcessBuilder) {
  val id = UUID.randomUUID().toString

  def exec[S <: Scheduling]()(implicit execution: Execution[S]): Future[Unit] = {
    val streams = execution.streams
    streams.debug(s"Forking:")
    streams.debug(this.toString)

    ExecutionPlatform
      .lookup[LocalPlatform[S]]
      .getOrElse(sys.error("No local execution platform configured"))
      .runInPool(execution) { () =>
        val result = Promise[Unit]()
        val handler = new NuAbstractProcessHandler() {
          override def onStdout(buffer: ByteBuffer, closed: Boolean) = {
            val bytes = Array.ofDim[Byte](buffer.remaining)
            buffer.get(bytes)
            streams.info(new String(bytes))
          }
          override def onStderr(buffer: ByteBuffer, closed: Boolean) = {
            val bytes = Array.ofDim[Byte](buffer.remaining)
            buffer.get(bytes)
            streams.error(new String(bytes))
          }
          override def onExit(statusCode: Int) =
            statusCode match {
              case 0 =>
                result.success(())
              case n =>
                result.failure(new Exception(s"Process exited with code $n"))
            }
        }
        process.setProcessListener(handler)
        val fork = process.start()
        streams.debug(s"... forked with PID ${fork.getPID}")
        execution.onCancelled(() => {
          fork.destroy(true)
        })
        result.future
      }
  }
}
