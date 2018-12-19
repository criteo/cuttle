package com.criteo.cuttle.cron

import java.time.Instant

import cats.effect.IO
import cats.implicits._
import com.criteo.cuttle._
import com.criteo.cuttle.utils.Timeout

import scala.concurrent.stm.{Ref, _}

/** A [[CronScheduler]] executes the set of Jobs at the time instants defined by Cron expressions.
  * Each [[com.criteo.cuttle.Job Job]] has it's own expression and executed separately from others.
  *
  * The [[com.criteo.cuttle.Scheduler Scheduler]] ensures that at least one
  * [[com.criteo.cuttle.Execution Execution]] is created and successfully run for a given time instant.
  * It also handles the retry policy.
  *
  * We follow the semantic of Cron on Unix systems and we don't manage misfires
  * (meaning that if the scheduler missed some events because it was offline,
  * or because a previous job execution was still running, we won't replay them).
  *
  * [[com.criteo.cuttle.Job Job]] is considered as finished when
  * we can't produce the next computing instant from Cron expression.
  */
case class CronScheduler(logger: Logger) extends Scheduler[CronScheduling] {
  private type CronExecution = Execution[CronScheduling]

  type CronJob = Job[CronScheduling]

  override val name = "cron"

  private val _state = Ref(Map.empty[CronJob, Either[Instant, CronExecution]])

  private def addNextEventToState(job: CronJob, instant: Instant) = IO {
    atomic { implicit txn =>
      _state() = _state() + (job -> Either.left(instant))
    }
  }

  private def addNextExecutionToState(job: CronJob, execution: CronExecution) = IO {
    atomic { implicit txn =>
      _state() = _state() + (job -> Either.right(execution))
    }
  }

  private def logState() = IO {
    val state = _state.single.get
    logger.debug("======State======")
    state.foreach {
      case (job, jobState) =>
        val messages = Seq(
          job.id,
          jobState.fold(_ toString, e => e.id)
        )
        logger.debug(messages mkString " :: ")
    }
    logger.debug("======End State======")
  }

  override def start(workload: Workload[CronScheduling],
                     executor: Executor[CronScheduling],
                     xa: XA,
                     logger: Logger = logger): Unit = {

    def runAndRetry(job: Job[CronScheduling], scheduledAt: ScheduledAt, retryNum: Int): IO[Completed] = {
      val runIO = for {
        runInfo <- IO {
          logger.debug(s"Sending job ${job.id} to executor")
          val cronContext = CronContext(scheduledAt.instant)(retryNum)
          executor.run(job, cronContext)
        }
        _ <- addNextExecutionToState(job, runInfo._1)
        _ <- logState()
        completed <- IO.fromFuture(IO(runInfo._2))
      } yield completed

      runIO.recoverWith {
        case e: Throwable =>
          if (retryNum < job.scheduling.maxRetry) {
            val nextRetry = retryNum + 1
            logger.debug(s"Job ${job.id} has failed it's going to be retried. Retry number: $nextRetry")
            runAndRetry(job, scheduledAt, nextRetry)
          } else {
            logger.debug(s"Job ${job.id} has reached the maximum number of retries")
            IO.raiseError(e)
          }
      }
    }

    def run(job: CronJob): IO[Completed] = {
      val runIO = for {
        maybeScheduledAt <- IO.pure(job.scheduling.nextEvent())
        completed <- maybeScheduledAt match {
          // we couldn't get next event from cron4s and it didn't fail so it means we've finished for this job
          case None =>
            logger.info(s"Job ${job.id} has finished. We will not submit executions anymore")
            IO.pure(Completed)
          // we got next event: update state with it, wait for it and run it or retry until max retry
          case Some(scheduledAt) =>
            logger.debug(s"Run job ${job.id} at ${scheduledAt.instant} with a delay of ${scheduledAt.delay}")
            for {
              _ <- addNextEventToState(job, scheduledAt.instant)
              _ <- logState()
              _ <- Timeout.applyF(scheduledAt.delay)
              _ <- runAndRetry(job, scheduledAt, 0)
              completed <- run(job)
            } yield completed
        }
      } yield completed

      runIO.recover {
        case e =>
          val message = s"Fatal error Cron loop for job ${job.id} will be stopped"
          logger.error(message)
          logger.error(e.getMessage)
          throw new Exception(message)
      }
    }

    val programs = workload match {
      case CronWorkload(jobs) =>
        logger.info(s"Building IOs for Cron Workload with ${jobs.size} job(s)")
        jobs.map(run)
    }

    // running all jobs asynchronously
    logger.info(s"Running Cron Workload")
    import ThreadPools.Implicits.cronContextShift
    programs.toList.parSequence.unsafeRunSync
    logger.info(s"Stopping Cron Workload")
  }
}
