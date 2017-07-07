package com.criteo.cuttle.platforms

import com.criteo.cuttle._

import scala.concurrent.stm._
import scala.concurrent.duration._

import java.time._

import lol.http._
import lol.json._

import io.circe._
import io.circe.syntax._

import App._

private[cuttle] object RateLimiter {
  val SC = fs2.Scheduler.fromFixedDaemonPool(1, "com.criteo.cuttle.platforms.RateLimiter.SC")
}

private[cuttle] class RateLimiter(tokens: Int, refillRateInMs: Int) extends WaitingExecutionQueue {
  private val _tokens = Ref(tokens)
  private val _lastRefill = Ref(Instant.now)

  RateLimiter.SC.scheduleAtFixedRate(refillRateInMs.milliseconds) {
    atomic { implicit txn =>
      if (_tokens() < tokens) {
        _tokens() = _tokens() + 1
        _lastRefill() = Instant.now
      }
    }
    runNext()
  }

  def canRunNextCondition(implicit txn: InTxn) = _tokens() >= 1
  def doRunNext()(implicit txn: InTxn) = _tokens() = _tokens() - 1

  override def routes(urlPrefix: String) = ({
    case req if req.url == urlPrefix =>
      Ok(Json.obj(
        "max_tokens" -> tokens.asJson,
        "available_tokens" -> _tokens.single.get.asJson,
        "refill_rate_in_ms" -> refillRateInMs.asJson,
        "last_refill" -> _lastRefill.single.get.asJson
      ))
  }: PartialService).orElse(super.routes(urlPrefix))

}
