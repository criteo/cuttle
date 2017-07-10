package com.criteo.cuttle.platforms.local

import java.nio.ByteBuffer
import java.util.UUID

import com.criteo.cuttle._
import com.criteo.cuttle.platforms.ExecutionPool
import com.zaxxer.nuprocess._
import lol.http.PartialService

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}

case class LocalPlatform(maxForkedProcesses: Int) extends ExecutionPlatform {
  private[local] val pool = new ExecutionPool(concurrencyLimit = maxForkedProcesses)

  override def waiting: Set[Execution[_]] =
    pool.waiting

  override lazy val publicRoutes: PartialService =
    pool.routes("/api/platforms/local/pool")
}

object LocalPlatform {
  def fork(command: String) = new LocalProcess(List("sh", "-ce", command))
}

class LocalProcess(command: List[String]) {
  val id = UUID.randomUUID().toString

  def exec[S <: Scheduling](env: Map[String, String] = sys.env)(implicit execution: Execution[S]): Future[Unit] = {
    val streams = execution.streams
    streams.debug(s"Forking:")
    streams.debug(this.toString)

    ExecutionPlatform
      .lookup[LocalPlatform]
      .getOrElse(sys.error("No local execution platform configured"))
      .pool
      .run(execution, debug = this.toString) { () =>
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
        val process = new NuProcessBuilder(command.asJava, env.asJava)
        process.setProcessListener(handler)
        val fork = process.start()
        streams.debug(s"forked with PID ${fork.getPID}")
        execution.onCancelled(() => {
          fork.destroy(true)
        })
        result.future
      }
  }
  override def toString = command.toString
}
