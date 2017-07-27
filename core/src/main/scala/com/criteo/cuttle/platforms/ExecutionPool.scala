package com.criteo.cuttle.platforms

import scala.concurrent.stm._

import lol.http._
import lol.json._

import io.circe._
import io.circe.syntax._

/**
  * A thread safe execution pool backed by a priority queue.
  *
  * Priority is based first on the [[Ordering]] of the Context, then the job id.
  *
  * @param name a globally unique name for this queue.
  * @param concurrencyLimit
  * @param contextOrdering
  */
private[cuttle] class ExecutionPool(concurrencyLimit: Int) extends WaitingExecutionQueue {
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
