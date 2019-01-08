package com.criteo.cuttle

import java.util.concurrent.atomic.AtomicInteger

import cats.effect.IO
import com.criteo.cuttle.ThreadPools._
import com.criteo.cuttle.ThreadPools.ThreadPoolSystemProperties._

import scala.concurrent.ExecutionContext

package object cron {
  type CronJob = Job[CronScheduling]
  type CronExecution = Execution[CronScheduling]

  object Implicits {
    // Thread pool to run Cron scheduler
    implicit val cronThreadPool = new WrappedThreadPool with Metrics {
      private val _threadPoolSize: AtomicInteger = new AtomicInteger(0)

      override val underlying = ExecutionContext.fromExecutorService(
        newFixedThreadPool(fromSystemProperties(CronThreadCount, Runtime.getRuntime.availableProcessors),
                           poolName = Some("Cron"),
                           threadCounter = _threadPoolSize)
      )

      override def threadPoolSize(): Int = _threadPoolSize.get()
    }

    implicit val cronContextShift = IO.contextShift(cronThreadPool.underlying)
  }
}
