package com.criteo.cuttle.platforms

import com.criteo.cuttle._

import scala.concurrent.stm._

import lol.http._
import lol.json._

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
private[cuttle] class ExecutionPool[S <: Scheduling](name: String, concurrencyLimit: Int)(
  implicit contextOrdering: Ordering[S#Context])
    extends WaitingExecutionQueue[S] {
  val queueOrdering = Ordering.by((e: Execution[S]) => (e.context, e.job.id))

  def canRunNextCondition(implicit txn: InTxn) = _running().size < concurrencyLimit
  def doRunNext()(implicit txn: InTxn): Unit = ()

  lazy val routes: PartialService = {
    case GET at url"/api/$name/tasks/_running" =>
      Ok(this._running.single.get.toSeq.map(_.toExecutionLog(ExecutionStatus.ExecutionRunning)).asJson)

    case GET at url"/api/$name/tasks/_waiting" =>
      Ok(this._waiting.single.get.toSeq.map(_._1.toExecutionLog(ExecutionStatus.ExecutionWaiting)).asJson)
  }

}
