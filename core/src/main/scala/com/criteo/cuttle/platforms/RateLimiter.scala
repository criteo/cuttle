package com.criteo.cuttle.platforms

import scala.concurrent.stm._
import scala.concurrent.duration._

private[cuttle] object RateLimiter {
  val SC = fs2.Scheduler.fromFixedDaemonPool(1, "com.criteo.cuttle.platforms.RateLimiter.SC")
}

private[cuttle] class RateLimiter(name: String, tokens: Int, refillRateInMs: Int) extends WaitingExecutionQueue {
  private val _tokens = Ref(tokens)

  RateLimiter.SC.scheduleAtFixedRate(refillRateInMs.milliseconds) {
    atomic { implicit txn =>
      if (_tokens() < tokens)
        _tokens() = _tokens() + 1
    }
    runNext()
  }

  def canRunNextCondition(implicit txn: InTxn) = _tokens() >= 1
  def doRunNext()(implicit txn: InTxn) = _tokens() = _tokens() - 1

}
