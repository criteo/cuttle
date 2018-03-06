package com.criteo.cuttle.platforms

import scala.concurrent.stm._

import lol.http._
import lol.json._

import io.circe._
import io.circe.syntax._

/**
  * An execution pool backed by a priority queue. It limits the concurrent executions
  * and the priority queue is ordered by the [[scala.math.Ordering Ordering]] defined
  * on the [[com.criteo.cuttle.SchedulingContext SchedulingContext]].
  *
  * @param concurrencyLimit The maximum number of concurrent executions.
  */
case class ExecutionPool(concurrencyLimit: Int) extends WaitingExecutionQueue {
  def canRunNextCondition(implicit txn: InTxn) = _running().size < concurrencyLimit
  def doRunNext()(implicit txn: InTxn): Unit = ()

  override def routes(urlPrefix: String) =
    ({
      case req if req.url == urlPrefix =>
        Ok(
          Json.obj(
            "concurrencyLimit" -> concurrencyLimit.asJson,
            "running" -> running.size.asJson,
            "waiting" -> waiting.size.asJson
          ))
    }: PartialService).orElse(super.routes(urlPrefix))
}
