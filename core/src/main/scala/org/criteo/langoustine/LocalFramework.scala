package org.criteo.langoustine

import java.nio.{ ByteBuffer }

import com.zaxxer.nuprocess._

import scala.concurrent.{ Future, Promise }
import scala.concurrent.stm.{ atomic, Ref }
import scala.collection.{ SortedSet }
import scala.concurrent.ExecutionContext.Implicits.global

case class LocalFramework(maxTasks: Int) extends ExecutionFramework {
  private val running = Ref(Set.empty[Execution[SchedulingContext]])
  private val waiting = Ref(SortedSet.empty[(Execution[SchedulingContext],() => Future[Unit],Promise[Unit])](
    Ordering.by(_._1.context)
  ))

  private def runNext(): Unit = {
    val maybeToRun = atomic { implicit txn =>
      if(running().size < 1) {
        val maybeNext = waiting().headOption
        maybeNext.foreach { case x @ (e, _, _) =>
          running() = running() + (e)
          waiting() = waiting() - x
        }
        maybeNext
      }
      else {
        None
      }
    }

    maybeToRun.foreach { case x @ (e, f, p) =>
      val fEffect = try { f() } catch {
        case t: Throwable =>
          Future.failed(t)
      }
      p.completeWith(fEffect)
      fEffect.andThen { case _ =>
        atomic { implicit txn =>
          running() = running() - e
        }
        runNext()
      }
    }
  }

  private[langoustine] def runInPool(e: Execution[SchedulingContext])(f: () => Future[Unit]): Future[Unit] = {
    val p = Promise[Unit]()
    atomic { implicit txn =>
      waiting() = waiting() + ((e,f,p))
    }
    runNext()
    p.future
  }
}

object LocalFramework {
  def fork(command: String) = new LocalProcess(new NuProcessBuilder("sh", "-c", command)) {
    override def toString = command
  }
}

class LocalProcess(private val process: NuProcessBuilder) {
  def exec()(implicit execution: Execution[SchedulingContext]): Future[Unit] = {
    val streams = execution.streams
    streams.debug(s"Waiting available resources to fork:")
    streams.debug(this.toString)
    streams.debug("...")
    
    ExecutionFramework.lookup[LocalFramework].getOrElse(sys.error("No local execution framework configured")).
      runInPool(execution) { () =>
        streams.debug("Running")
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
          override def onExit(statusCode: Int) = {
            statusCode match {
              case 0 =>
                result.success(())
              case n =>
                result.failure(new Exception(s"Status code $n"))
            }
          }
        }
        process.setProcessListener(handler)
        process.start()
        result.future
      }
  }
}