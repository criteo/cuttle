package com.criteo.cuttle.cron

import java.time.Instant

import cats.effect.IO
import cats.implicits._
import com.criteo.cuttle.ThreadPools.Implicits.sideEffectThreadPool
import com.criteo.cuttle.ThreadPools._
import com.criteo.cuttle._
import com.criteo.cuttle.utils.Timeout

import scala.concurrent.stm.Ref
import scala.concurrent.stm._

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

  private sealed trait CronJob {
    val underlying: Job[CronScheduling]
    def nextEvent = underlying.scheduling.nextEvent()
  }
  private case class New(override val underlying: Job[CronScheduling]) extends CronJob
  private case class Retry(override val underlying: Job[CronScheduling], retryNum: Int, scheduledAt: ScheduledAt)
      extends CronJob

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

  private def removeJobFromState(job: CronJob) = IO {
    atomic { implicit txn =>
      _state() = _state() - job
    }
  }

  private def logState() = IO {
    val state = _state.single.get
    logger.debug("======State======")
    state.foreach {
      case (job, jobState) =>
        val messages = Seq(
          job.underlying.id,
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
    def runScheduledJob(job: CronJob, retryNum: Int, scheduledAt: ScheduledAt) =
      for {
        runInfo <- IO {
          logger.debug(s"Sending job ${job.underlying.id} to executor")
          val cronContext = CronContext(scheduledAt.instant)(retryNum)
          executor.run(job.underlying, cronContext)
        }
        _ <- addNextExecutionToState(job, runInfo._1)
        _ <- logState()
        result <- IO.fromFuture(IO(runInfo._2))
      } yield result

    def run(job: CronJob): IO[Completed] = {
      val maybeNextJob = job match {
        case newJob @ New(underlying) =>
          for {
            maybeScheduledAt <- IO.pure(job.nextEvent)
            nextJob <- maybeScheduledAt match {
              case None => IO.pure(None)
              case Some(scheduledAt) =>
                logger.debug(
                  s"Run job ${newJob.underlying.id} at ${scheduledAt.instant} with a delay of ${scheduledAt.delay}")
                for {
                  _ <- addNextEventToState(job, scheduledAt.instant)
                  _ <- logState()
                  _ <- Timeout.applyF(scheduledAt.delay)

                  nextJob <- runScheduledJob(newJob, 0, scheduledAt)
                    .map(_ => Some(newJob: CronJob))
                    .recover {
                      case _ if newJob.underlying.scheduling.maxRetry > 0 =>
                        logger.debug(s"Job ${underlying.id} has failed it's going to be retried")
                        Some(Retry(underlying, 1, scheduledAt))
                      case e =>
                        logger.debug(s"Job ${underlying.id} has reached the maximum number of retries")
                        Some(New(underlying))
                    }
                } yield nextJob
            }
            _ <- removeJobFromState(newJob)
          } yield nextJob

        case retryJob @ Retry(underlying, retryNum, scheduledAt) =>
          logger.debug(s"Retry job ${retryJob.underlying.id} at ${scheduledAt.instant}")
          for {
            nextJob <- runScheduledJob(retryJob, retryNum, scheduledAt)
              .map(_ => Some(New(underlying): CronJob))
              .recover {
                case _ if underlying.scheduling.maxRetry >= retryNum =>
                  logger.debug(
                    s"Job ${retryJob.underlying.id} has failed it's going to be retried. Retry number: $retryNum")
                  Some(Retry(underlying, retryNum + 1, scheduledAt))
                case e =>
                  logger.debug(s"Job ${underlying.id} has reached the maximum number of retries")
                  Some(New(underlying))
              }
            _ <- removeJobFromState(retryJob)
          } yield nextJob
      }

      maybeNextJob
        .flatMap {
          case None =>
            logger.info(s"Job ${job.underlying.id} has finished. We will not submit executions anymore")
            IO.pure(Completed)
          case Some(nextJob) => run(nextJob)
        }
        .recover {
          case e =>
            val message = s"Fatal error Cron loop for job ${job.underlying.id} will be stopped"
            logger.error(message)
            throw new Exception(message)
        }
    }

    val programs = workload match {
      case CronWorkload(jobs) => jobs.map(New).map(run)
    }

    programs.foreach(_.unsafeRunAsync(_ => ()))
  }
}
