package com.criteo.cuttle

import java.nio.{ByteBuffer}

import com.zaxxer.nuprocess._

import java.util.{UUID}

import scala.concurrent.{Future, Promise}
import scala.concurrent.stm.{atomic, Ref}
import scala.collection.{SortedSet}
import scala.concurrent.ExecutionContext.Implicits.global

import ExecutionStatus._

case class LocalPlatform[S <: Scheduling](maxForkedProcesses: Int)(implicit contextOrdering: Ordering[S#Context])
    extends ExecutionPlatform[S]
    with LocalPlatformApp[S] {
  private[cuttle] val _running = Ref(Set.empty[(LocalProcess, Execution[S])])
  private[cuttle] val _waiting = Ref(
    SortedSet.empty[(LocalProcess, Execution[S], () => Future[Unit], Promise[Unit])](Ordering.by(x =>
      (x._2.context, x._2.job.id))))

  def waiting = _waiting.single().map(_._2)

  private def runNext(): Unit = {
    val maybeToRun = atomic { implicit txn =>
      if (_running().size < maxForkedProcesses) {
        val maybeNext = _waiting().headOption
        maybeNext.foreach {
          case x @ (l, e, _, _) =>
            _running() = _running() + (l -> e)
            _waiting() = _waiting() - x
        }
        maybeNext
      } else {
        None
      }
    }

    maybeToRun.foreach {
      case x @ (l, e, f, p) =>
        val fEffect = try { f() } catch {
          case t: Throwable =>
            Future.failed(t)
        }
        p.completeWith(fEffect)
        fEffect.andThen {
          case _ =>
            atomic { implicit txn =>
              _running() = _running() - (l -> e)
            }
            runNext()
        }
    }
  }

  private[cuttle] def runInPool(l: LocalProcess, e: Execution[S])(f: () => Future[Unit]): Future[Unit] = {
    val p = Promise[Unit]()
    val entry = (l, e, f, p)
    atomic { implicit txn =>
      _waiting() = _waiting() + entry
    }
    e.onCancelled(() => {
      atomic { implicit txn =>
        _waiting() = _waiting() - entry
      }
      p.tryFailure(ExecutionCancelledException)
    })
    runNext()
    p.future
  }

}

object LocalPlatform {
  def fork(command: String) = new LocalProcess(new NuProcessBuilder("sh", "-c", command)) {
    override def toString = command
  }
}

private[cuttle] trait LocalPlatformApp[S <: Scheduling] { self: LocalPlatform[S] =>

  import lol.http._
  import lol.json._

  import io.circe._
  import io.circe.syntax._

  import App._

  implicit val encoder = new Encoder[(LocalProcess, Execution[S])] {
    override def apply(x: (LocalProcess, Execution[S])) = x match {
      case (process, execution) =>
        Json.obj(
          "id" -> process.id.asJson,
          "command" -> process.toString.asJson,
          "execution" -> execution.toExecutionLog(ExecutionRunning).asJson
        )
    }
  }

  override lazy val routes: PartialService = {
    case GET at url"/api/local/tasks/_running" =>
      Ok(this._running.single.get.toSeq.asJson)
    case GET at url"/api/local/tasks/_waiting" =>
      Ok(this._waiting.single.get.toSeq.map(x => (x._1, x._2)).asJson)
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
      .runInPool(this, execution) { () =>
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
