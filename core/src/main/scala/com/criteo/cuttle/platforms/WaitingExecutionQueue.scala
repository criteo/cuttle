package com.criteo.cuttle.platforms

import com.criteo.cuttle._

import java.time._

import lol.http._
import lol.json._

import scala.concurrent._
import scala.concurrent.stm._
import scala.collection.{SortedSet}

import scala.util._
import scala.concurrent.ExecutionContext.Implicits.global

import io.circe._
import io.circe.syntax._

import App._

/** A priority queue ordered by [[com.criteo.cuttle.SchedulingContext SchedulingContext]] priority. */
trait WaitingExecutionQueue {
  case class DelayedResult[A](effect: () => Future[A],
                              effectDebug: String,
                              promise: Promise[A],
                              submitted: Instant,
                              started: Ref[Option[Instant]] = Ref(None))

  implicit def taskEncoder[A] = new Encoder[DelayedResult[A]] {
    override def apply(task: DelayedResult[A]) =
      Json.obj(
        "effect" -> task.effectDebug.toString.asJson,
        "submitted" -> task.submitted.asJson,
        "started" -> task.started.single.get.asJson
      )
  }

  lazy val _running = Ref(Set.empty[(Execution[_], DelayedResult[_])])
  lazy val _waiting = Ref(
    SortedSet.empty[(Execution[_ <: Scheduling], DelayedResult[_])](
      Ordering.by[(Execution[_ <: Scheduling], DelayedResult[_]), (Execution[_], Int)]({
        case (execution, delayedResult) => execution -> delayedResult.hashCode
      })))

  def waiting: Set[Execution[_]] = _waiting.single().map(_._1)
  def running: Set[Execution[_]] = _running.single().map(_._1)

  def canRunNextCondition(implicit txn: InTxn): Boolean
  def doRunNext()(implicit txn: InTxn): Unit

  def run[A, S <: Scheduling](execution: Execution[S], debug: String)(f: () => Future[A]): Future[A] = {
    val result = DelayedResult(f, debug, Promise[A](), Instant.now)
    val entry = (execution, result)
    atomic { implicit txn =>
      _waiting() = _waiting() + entry
    }
    execution
      .onCancel(() => {
        // we cancel only waiting executions because the running ones are not in responsibility of the queue.
        val wasWaiting = atomic { implicit txn =>
          val wasWaiting = _waiting().contains(entry)
          _waiting() = _waiting() - entry
          wasWaiting
        }
        if (wasWaiting) result.promise.tryComplete(Failure(ExecutionCancelled))
      })
      .unsubscribeOn(result.promise.future)
    runNext()
    result.promise.future
  }

  def runNext(): Unit = {
    val maybeToRun = atomic { implicit txn =>
      if (canRunNextCondition) {
        val maybeNext = _waiting().headOption
        maybeNext.foreach {
          case entry =>
            _running() = _running() + entry
            _waiting() = _waiting() - entry
            entry._2.started() = Some(Instant.now)
            doRunNext()
        }
        maybeNext
      } else {
        None
      }
    }

    maybeToRun.foreach {
      case entry @ (_, DelayedResult(effect, _, promise, _, _)) =>
        val effectResult = try {
          effect()
        } catch {
          case t: Throwable =>
            Future.failed(t)
        }
        promise.tryCompleteWith(effectResult)
        effectResult.andThen {
          case _ =>
            atomic { implicit txn =>
              _running() = _running() - entry
            }
            runNext()
        }
    }
  }

  def routes(urlPrefix: String): PartialService = {
    case req if req.url == s"$urlPrefix/running" =>
      Ok(this._running.single.get.toSeq.map {
        case (execution, task) =>
          Json.obj(
            "execution" -> execution.toExecutionLog(ExecutionStatus.ExecutionRunning).asJson,
            "task" -> task.asJson
          )
      }.asJson)
    case req if req.url == s"$urlPrefix/waiting" =>
      Ok(this._waiting.single.get.toSeq.map {
        case (execution, task) =>
          Json.obj(
            "execution" -> execution.toExecutionLog(ExecutionStatus.ExecutionWaiting).asJson,
            "task" -> task.asJson
          )
      }.asJson)
  }
}
