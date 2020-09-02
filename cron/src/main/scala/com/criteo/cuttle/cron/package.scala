package com.criteo.cuttle

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import cats.effect.IO
import com.criteo.cuttle.Auth.User
import com.criteo.cuttle.ThreadPools.ThreadPoolSystemProperties._
import com.criteo.cuttle.ThreadPools._

import scala.concurrent.ExecutionContext
import scala.language.implicitConversions

package object cron {
  type CronJob = Job[CronScheduling]
  type CronExecution = Execution[CronScheduling]

  // In the Cron scheduler, we do not pause jobs, we pause entire DAGs
  type PausedDag = PausedJob
  object PausedDag {
    def apply(id: String, user: User, date: Instant): PausedDag = PausedJob(id, user, date)
  }

  object Implicits {

    //Backward compat for Job to CronDag
    implicit class JobToCronDag(job: Job[CronScheduling]) {
      def every(cronExpression: CronExpression) =
        CronDag(job.id, CronPipeline(Set(job), Set.empty), cronExpression, job.name, job.description, job.tags)
    }

    implicit def stringToCronExp(cronExpression: String) = CronExpression(cronExpression)

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

}
