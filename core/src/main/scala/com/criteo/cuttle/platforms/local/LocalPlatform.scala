package com.criteo.cuttle.platforms.local

import java.nio.ByteBuffer

import com.criteo.cuttle._
import com.criteo.cuttle.platforms.ExecutionPool
import com.zaxxer.nuprocess._

import cats.effect._

import org.http4s._
import org.http4s._, org.http4s.dsl.io._, org.http4s.implicits._
import org.http4s.circe._

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}

/** The [[LocalPlatform]] handles the execution of locally forked processes.
  * The number of concurrently forked processes is limited and can be configured for
  * the platform.
  *
  * While waiting for the process to be actually forked, the [[com.criteo.cuttle.Job Job]]
  * [[com.criteo.cuttle.Execution Execution]] is seen as __WAITING__ in the UI.
  *
  * @param maxForkedProcesses The maximum number of concurrently running processes.
  **/
case class LocalPlatform(maxForkedProcesses: Int) extends ExecutionPlatform {
  private[local] val pool = new ExecutionPool(concurrencyLimit = maxForkedProcesses)

  override def waiting: Set[Execution[_]] =
    pool.waiting

  override lazy val publicRoutes: HttpRoutes[IO] =
    pool.routes("/api/platforms/local/pool")
}

/** Represent a process to be locally foked.
  *
  * @param command The actual command to be run in a new `shell`.
  */
class LocalProcess(command: String) {

  private def exec0[S <: Scheduling](
    env: Map[String, String],
    outLogger: (String) => Unit,
    errLogger: (String) => Unit
  )(implicit execution: Execution[S]): Future[Completed] = {
    val streams = execution.streams
    streams.debug(s"Forking:")
    streams.debug(this.toString)

    ExecutionPlatform
      .lookup[LocalPlatform]
      .getOrElse(sys.error("No local execution platform configured"))
      .pool
      .run(execution, debug = this.toString) { () =>
        val result = Promise[Completed]()
        val handler = new NuAbstractProcessHandler() {
          override def onStdout(buffer: ByteBuffer, closed: Boolean) = {
            val bytes = Array.ofDim[Byte](buffer.remaining)
            buffer.get(bytes)
            val str = new String(bytes)
            streams.info(str)
            outLogger(str)
          }
          override def onStderr(buffer: ByteBuffer, closed: Boolean) = {
            val bytes = Array.ofDim[Byte](buffer.remaining)
            buffer.get(bytes)
            val str = new String(bytes)
            if (str.nonEmpty) {
              streams.error(str)
              errLogger(str)
            }
          }
          override def onExit(statusCode: Int) =
            statusCode match {
              case 0 =>
                result.success(Completed)
              case n =>
                result.failure(new Exception(s"Process exited with code $n"))
            }
        }
        val process = new NuProcessBuilder(List("sh", "-c", s"exec ${command.trim}").asJava, env.asJava)
        process.setProcessListener(handler)
        val fork = process.start()
        streams.debug(s"forked with PID ${fork.getPID}")
        execution
          .onCancel(() => {
            fork.destroy(false)
          })
          .unsubscribeOn(result.future)
        result.future
      }
  }

  /** Fork this process for the given [[com.criteo.cuttle.Execution Execution]]. The returned
    * [[scala.concurrent.Future Future]] will be resolved as soon as the command complete.
    *
    * @param env The environment variables to pass to the forked process
    * @param execution The execution for which this process is forked. The process out will be redirected to
    *                  the [[com.criteo.cuttle.ExecutionStreams execution streams]].
    */
  def apply[S <: Scheduling](env: Map[String, String] = sys.env)(implicit execution: Execution[S]): Future[Completed] =
    exec0(env, _ => (), _ => ())

  /** Fork this process for the given [[com.criteo.cuttle.Execution Execution]]. The returned
    * [[scala.concurrent.Future Future]] will be resolved as soon as the command complete.
    *
    * All the output of the forked process (STDOUT,STERR) will be buffered in memory and sent back as the
    * result of the returned future.
    *
    * __BECAUSE EVERYTHING WILL BE BUFFERED IN MEMORY YOU DON'T WANT TO DO THAT FOR PROCESS WITH LARGE OUTPUT__
    *
    * @param env The environment variables to pass to the forked process
    * @param execution The execution for which this process is forked. The process out will be redirected to
    *                  the [[com.criteo.cuttle.ExecutionStreams execution streams]].
    */
  def execAndRetrieveOutput[S <: Scheduling](
    env: Map[String, String] = sys.env
  )(implicit execution: Execution[S]): Future[(String, String)] = {
    val out = new StringBuffer
    val err = new StringBuffer
    exec0(env, x => out.append(x), x => err.append(x)).map(_ => (out.toString, err.toString))
  }

  override def toString = command
}
