package com.criteo.cuttle.platforms

import com.criteo.cuttle._

import scala.concurrent._
import scala.concurrent.stm._
import scala.collection.{SortedSet}

import scala.concurrent.ExecutionContext.Implicits.global

private[cuttle] trait WaitingExecutionQueue {
  case class DelayedResult[A](effect: () => Future[A], promise: Promise[A])

  lazy val _running = Ref(Set.empty[Execution[_]])
  lazy val _waiting = Ref(SortedSet.empty[(Execution[_], DelayedResult[_])](Ordering.by(_._1)))

  def waiting: Set[Execution[_]] = _waiting.single().map(_._1)

  def canRunNextCondition(implicit txn: InTxn): Boolean
  def doRunNext()(implicit txn: InTxn): Unit

  def run[A, S <: Scheduling](execution: Execution[S])(f: () => Future[A]): Future[A] = {
    val result = DelayedResult(f, Promise[A]())
    val entry = (execution, result)
    atomic { implicit txn =>
      _waiting() = _waiting() + entry
    }
    execution.onCancelled(() => {
      atomic { implicit txn =>
        _waiting() = _waiting() - entry
      }
      result.promise.tryCompleteWith(execution.cancelled)
    })
    runNext()
    result.promise.future
  }

  def runNext(): Unit = {
    val maybeToRun = atomic { implicit txn =>
      if (canRunNextCondition) {
        val maybeNext = _waiting().headOption
        maybeNext.foreach {
          case entry @ (execution, _) =>
            _running() = _running() + execution
            _waiting() = _waiting() - entry
            doRunNext()
        }
        maybeNext
      } else {
        None
      }
    }

    maybeToRun.foreach {
      case entry @ (execution, DelayedResult(effect, promise)) =>
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
              _running() = _running() - execution
            }
            runNext()
        }
    }
  }
}
