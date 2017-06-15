package com.criteo.cuttle.platforms

import com.criteo.cuttle.ExecutionStatus.ExecutionRunning
import com.criteo.cuttle.{App, Execution, Scheduling}

import scala.collection.SortedSet
import scala.concurrent.{Future, Promise}
import scala.concurrent.stm.{Ref, atomic}
import scala.concurrent.ExecutionContext.Implicits.global

import lol.http._
import lol.json._

import io.circe._
import io.circe.syntax._

import App._

/**
  * A thread safe execution pool backed by a priority queue.
  *
  * Priority is based first on the [[Ordering]] of the Context, then the job id.
  *
  * @param name a globally unique name for this queue.
  * @param concurrencyLimit
  * @param contextOrdering
  * @tparam S
  */
class ExecutionPool[S <: Scheduling](name: String, concurrencyLimit: Int)(implicit contextOrdering: Ordering[S#Context]) {

  private implicit val encoder = ExecutionPool.encoder[S]

  private val _running = Ref(Set.empty[Execution[S]])

  private val _waiting = Ref(
    SortedSet.empty[(Execution[S], () => Future[Unit], Promise[Unit])](Ordering.by(x =>
      (x._1.context, x._1.job.id))))

  /** The currently waiting executions. */
  def waiting: Set[Execution[S]] = _waiting.single().map(_._1)

  /** Exposes an HTTP API for inspecting the state of the queue. */
  lazy val routes: PartialService = {
    case GET at url"/api/$name/tasks/_running" =>
      Ok(this._running.single.get.toSeq.asJson)

    case GET at url"/api/$name/tasks/_waiting" =>
      Ok(this._waiting.single.get.toSeq.map(_._1).asJson)
  }

  /**
    * The entry point to add an [[Execution]] into the queue.
    *
    * @param e the [[Execution]] context for this invocation.
    * @param f the *non* blocking function to be called when resources are available.
    * @return
    */
  def runInPool(e: Execution[S])(f: () => Future[Unit]): Future[Unit] = {
    val p = Promise[Unit]()
    val entry = (e, f, p)
    atomic { implicit txn =>
      _waiting() = _waiting() + entry
    }
    e.onCancelled(() => {
      atomic { implicit txn =>
        _waiting() = _waiting() - entry
      }
      p.tryCompleteWith(e.cancelled)
    })
    runNext()
    p.future
  }

  /** finds the next Execution to run. */
  private def runNext(): Unit = {
    val maybeToRun = atomic { implicit txn =>
      if (_running().size < concurrencyLimit) {
        val maybeNext = _waiting().headOption
        maybeNext.foreach {
          case x@(e, _, _) =>
            _running() = _running() + e
            _waiting() = _waiting() - x
        }
        maybeNext
      } else {
        None
      }
    }

    maybeToRun.foreach {
      case x@(e, f, p) =>
        val fEffect = try {
          f()
        } catch {
          case t: Throwable =>
            Future.failed(t)
        }
        p.completeWith(fEffect)
        fEffect.andThen {
          case _ =>
            atomic { implicit txn =>
              _running() = _running() - e
            }
            runNext()
        }
    }
  }

}

object ExecutionPool {
  def encoder[S <: Scheduling]: Encoder[Execution[S]] = new Encoder[Execution[S]] {
    override def apply(x: Execution[S]): Json =
      Json.obj(
        "id" -> x.id.asJson,
        "name" -> x.job.name.asJson,
        "description" -> x.job.description.asJson,
        "execution" -> x.toExecutionLog(ExecutionRunning).asJson
      )
  }
}
