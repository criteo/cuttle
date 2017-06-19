package com.criteo.cuttle.platforms

import com.criteo.cuttle._

import scala.concurrent.stm._
import scala.concurrent.duration._

private[cuttle] object RateLimiter {
  val SC = fs2.Scheduler.fromFixedDaemonPool(1, "com.criteo.cuttle.platforms.RateLimiter.SC")
}

private[cuttle] class RateLimiter[S <: Scheduling](name: String, tokens: Int, refillRateInMs: Int)(
  implicit contextOrdering: Ordering[S#Context])
    extends WaitingExecutionQueue[S] {
  val queueOrdering = Ordering.by((e: Execution[S]) => (e.context, e.job.id))
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
