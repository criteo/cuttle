package com.criteo.cuttle

import scala.concurrent.duration._

object WaitingTimesMonitor extends Queries {
  def start[S <: Scheduling](executor: Executor[S], xa: XA) = {
    val SC = fs2.Scheduler.fromFixedDaemonPool(1, "com.criteo.cuttle.platforms.ExecutionMonitor.SC")

    val intervalSeconds = 1

    SC.scheduleAtFixedRate(intervalSeconds.second) {
      executor
        .runningExecutions
        .filter({ case (_, s) => s == ExecutionStatus.ExecutionWaiting })
        .foreach({ case (e, _) => e.updateWaitingTime(intervalSeconds) })
    }
  }
}
