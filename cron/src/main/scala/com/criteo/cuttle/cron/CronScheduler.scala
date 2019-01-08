package com.criteo.cuttle.cron

import scala.concurrent.stm.Txn.ExternalDecider
import scala.concurrent.stm._
import cats.effect.IO
import cats.implicits._
import doobie.implicits._
import io.circe.Json

import com.criteo.cuttle._
import com.criteo.cuttle.utils.Timeout

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

  override val name = "cron"

  private val queries = new Queries {}
  private val state = CronState(logger)

  private def logState = IO(logger.debug(state.toString()))

  /**
    * We associate a commit of new STM state with a DB commit.
    * It means that STM state commit only happens whether we commit to DB successfully.
    * That allows us to keep STM and DB in sync.
    * @param dbConnection doobie DB connection
    * @param transactor  doobie transactor
    * @param txn current STM transaction context
    */
  private def setExernalDecider[A](dbConnection: doobie.ConnectionIO[A])(implicit transactor: XA, txn: InTxnEnd): Unit =
    Txn.setExternalDecider(new ExternalDecider {
      def shouldCommit(implicit txn: InTxnEnd): Boolean = {
        dbConnection.transact(transactor).unsafeRunSync
        true
      }
    })

  private[cron] def getPausedJobs = state.getPausedJobs()

  private[cron] def pauseJobs(jobs: Set[CronJob], executor: Executor[CronScheduling])(implicit transactor: XA,
                                                                                      user: Auth.User): Unit = {
    logger.debug(s"Pausing jobs $jobs")
    val cancelableExecutions = atomic { implicit tx =>
      val jobsToPause = state.pauseJobs(jobs)

      if (jobsToPause.isEmpty) Left(Seq.empty)
      else {
        val pauseQuery = jobsToPause.map(queries.pauseJob).reduceLeft(_ *> _)
        setExernalDecider(pauseQuery)

        Right(jobsToPause.flatMap { toPause =>
          logger.debug(s"Retrieve executions to pause for $toPause")
          executor.runningState.filterKeys(_.job.id == toPause.id).keys ++ executor.throttledState
            .filterKeys(_.job.id == toPause.id)
            .keys
        })
      }
    }

    cancelableExecutions match {
      case Left(_) => ()
      case Right(executions) =>
        logger.debug(s"Canceling ${executions.size} executions")
        executions.foreach { execution =>
          execution.streams.debug(s"Job has been paused by user ${user.userId}")
          execution.cancel()
        }
    }
  }

  private[cron] def resumeJobs(jobs: Set[Job[CronScheduling]],
                               executor: Executor[CronScheduling])(implicit transactor: XA, user: Auth.User): Unit = {
    logger.debug(s"Resuming jobs $jobs")
    val jobIdsToResume = jobs.map(_.id)
    val resumeQuery = jobIdsToResume.map(queries.resumeJob).reduceLeft(_ *> _)

    atomic { implicit tx =>
      setExernalDecider(resumeQuery)
      state.resumeJobs(jobs)
    }
    val programs = jobs.map { job =>
      logger.debug(s"Activating job $job")
      run(job, executor)
    }

    logger.info(s"Relaunching jobs $jobs")
    unsafeRunAsync(programs)
  }

  private def run(job: CronJob, executor: Executor[CronScheduling]): IO[Completed] = {
    def runAndRetry(job: Job[CronScheduling], scheduledAt: ScheduledAt, retryNum: Int): IO[Completed] =
      // don't run anything when job is paused
      if (state.isPaused(job)) {
        IO.pure(Completed)
      } else {
        val runIO = for {
          runInfo <- IO {
            logger.debug(s"Sending job ${job.id} to executor")
            val cronContext = CronContext(scheduledAt.instant)(retryNum)
            executor.run(job, cronContext)
          }
          _ <- IO(state.addNextExecutionToState(job, runInfo._1))
          _ <- logState
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

    // don't run anything when job is paused
    if (state.isPaused(job)) {
      IO.pure(Completed)
    } else {
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
              _ <- IO(state.addNextEventToState(job, scheduledAt.instant))
              _ <- logState
              _ <- Timeout.applyF(scheduledAt.delay)
              _ <- runAndRetry(job, scheduledAt, 0)
              completed <- run(job, executor)
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
  }

  private def unsafeRunAsync(programs: Set[IO[Completed]]) = {
    import com.criteo.cuttle.cron.Implicits._
    programs.toList.parSequence.unsafeRunAsyncAndForget()
  }

  override def getStats(jobIds: Set[String]): Json = state.snapshot(jobIds)

  override def start(workload: Workload[CronScheduling],
                     executor: Executor[CronScheduling],
                     xa: XA,
                     logger: Logger = logger): Unit = {
    logger.info("Getting paused jobs")
    val pausedJob = queries.getPausedJobs.transact(xa).unsafeRunSync()

    val programs = workload match {
      case CronWorkload(jobs) =>
        logger.info("Init Cron State")
        state.init(jobs, pausedJob)
        logger.info(s"Building IOs for Cron Workload with ${jobs.size} job(s)")
        jobs.map(job => run(job, executor))
    }

    // running all jobs asynchronously
    logger.info(s"Launching Cron Workload")
    unsafeRunAsync(programs)
  }
}
