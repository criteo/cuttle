package com.criteo.cuttle.platforms

import scala.concurrent.stm._
import scala.concurrent.duration._
import java.time._

import lol.http._
import lol.json._
import io.circe._
import io.circe.syntax._
import io.circe.java8.time._
import cats.effect.IO

import com.criteo.cuttle.utils.timer

/**
  * An rate limiter pool backed by a priority queue. It rate limits the executions
  * and the priority queue is ordered by the [[scala.math.Ordering Ordering]] defined
  * on the [[com.criteo.cuttle.SchedulingContext SchedulingContext]].
  *
  * The implementation is based on the tokens bucket algorithm.
  *
  * @param tokens Maximum (and initial) number of tokens.
  * @param refillRateInMs A token is added to the bucket every `refillRateInMs` milliseconds.
  */
class RateLimiter(tokens: Int, refillRateInMs: Int) extends WaitingExecutionQueue {
  private val _tokens = Ref(tokens)
  private val _lastRefill = Ref(Instant.now)

  fs2.Stream.awakeEvery[IO](refillRateInMs.milliseconds)
    .flatMap(_ => {
      atomic { implicit txn =>
        if (_tokens() < tokens) {
          _tokens() = _tokens() + 1
          _lastRefill() = Instant.now
        }
      }
      fs2.Stream(runNext())
    })
    .compile
    .drain
    .unsafeRunAsync(_ => ())

  def canRunNextCondition(implicit txn: InTxn) = _tokens() >= 1
  def doRunNext()(implicit txn: InTxn) = _tokens() = _tokens() - 1

  override def routes(urlPrefix: String) =
    ({
      case req if req.url == urlPrefix =>
        Ok(
          Json.obj(
            "max_tokens" -> tokens.asJson,
            "available_tokens" -> _tokens.single.get.asJson,
            "refill_rate_in_ms" -> refillRateInMs.asJson,
            "last_refill" -> _lastRefill.single.get.asJson
          ))
    }: PartialService).orElse(super.routes(urlPrefix))

}
