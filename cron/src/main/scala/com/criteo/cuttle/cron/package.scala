package com.criteo.cuttle

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.ExecutionContext
import cats.effect.IO
import com.criteo.cuttle.ThreadPools._
import com.criteo.cuttle.ThreadPools.ThreadPoolSystemProperties._

package object cron {
  type CronJob = Job[CronScheduling]
  type CronExecution = Execution[CronScheduling]

  object Implicits {
    // Thread pool to run Cron scheduler
    implicit val cronThreadPool = new WrappedThreadPool with Metrics {
      private val _threadPoolSize: AtomicInteger = new AtomicInteger(0)

      override val underlying = ExecutionContext.fromExecutorService(
        newFixedThreadPool(
          loadSystemPropertyAsInt("com.criteo.cuttle.ThreadPools.CronThreadPool.nThreads",
                                  Runtime.getRuntime.availableProcessors),
          poolName = Some("Cron"),
          threadCounter = _threadPoolSize
        )
      )

      override def threadPoolSize(): Int = _threadPoolSize.get()
    }

    implicit val cronContextShift = IO.contextShift(cronThreadPool.underlying)
  }

  // Fair assumptions about start and end date within which we operate by default if user doesn't specify his interval.
  // We choose these dates over Instant.MIN and Instant.MAX because MySQL works within this range.
  private[cron] val minStartDateForExecutions = Instant.parse("1000-01-01T00:00:00Z")
  private[cron] val maxStartDateForExecutions = Instant.parse("9999-12-31T23:59:59Z")

  // This function was implemented because executor.archivedExecutions returns duplicates when passing the same table
  // into the context query.
  private[cron] def buildExecutionsList(executor: Executor[CronScheduling],
                                        job: CronJob,
                                        startDate: Instant,
                                        endDate: Instant,
                                        limit: Int)(implicit transactor: XA) =
    for {
      archived <- executor.rawArchivedExecutions(Set(job.id), "", asc = false, 0, limit, transactor)
      running <- IO(executor.runningExecutions.collect {
        case (e, status)
            if e.job.id == job.id && e.context.instant.isAfter(startDate) && e.context.instant.isBefore(endDate) =>
          e.toExecutionLog(status)
      })
    } yield (running ++ archived)
}
