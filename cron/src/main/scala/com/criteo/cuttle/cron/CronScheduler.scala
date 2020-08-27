package com.criteo.cuttle.cron

import java.time.Instant
import java.util.concurrent.TimeUnit

import cats.effect.IO
import cats.effect.concurrent.Deferred
import cats.implicits._
import com.criteo.cuttle._
import com.criteo.cuttle.cron.Implicits._
import com.criteo.cuttle.utils._
import doobie.implicits._
import io.circe.Json

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.stm.{atomic, InTxnEnd, Txn}

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

  private val queries = Queries(logger)

  private val state = CronState(logger)

  private def logState = IO(logger.debug(state.toString()))

  /**
    * We associate a commit of new STM state with a DB commit.
    * It means that STM state commit only happens whether we commit to DB successfully.
    * That allows us to keep STM and DB in sync.
    *
    * @param dbConnection doobie DB connection
    * @param transactor   doobie transactor
    * @param txn          current STM transaction context
    */
  private def setExernalDecider[A](dbConnection: doobie.ConnectionIO[A])(implicit transactor: XA, txn: InTxnEnd): Unit =
    Txn.setExternalDecider(new Txn.ExternalDecider {
      def shouldCommit(implicit txn: InTxnEnd): Boolean = {
        dbConnection.transact(transactor).unsafeRunSync
        true
      }
    })

  private[cron] def getPausedDags = state.getPausedDags()

  private[cron] def snapshot(dagIds: Set[String]) = state.snapshot(dagIds)

  private[cron] def pauseDags(dags: Set[CronDag], executor: Executor[CronScheduling])(implicit transactor: XA,
                                                                                      user: Auth.User): Unit = {
    logger.debug(s"Pausing job DAGs $dags")
    val cancelableExecutions = atomic { implicit tx =>
      val dagsToPause = state.pauseDags(dags)

      if (dagsToPause.isEmpty) Left(Seq.empty)
      else {
        val pauseQuery = dagsToPause.map(queries.pauseJob).reduceLeft(_ *> _)
        setExernalDecider(pauseQuery)

        Right(dagsToPause.flatMap { toPause =>
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

  private[cron] def resumeDags(dags: Set[CronDag], executor: Executor[CronScheduling])(implicit transactor: XA,
                                                                                       user: Auth.User): Unit = {
    logger.debug(s"Resuming job DAGs $dags")
    val dagIdsToExecute = dags.map(_.id)
    val resumeQuery = dagIdsToExecute.map(queries.resumeJob).reduceLeft(_ *> _)

    atomic { implicit tx =>
      setExernalDecider(resumeQuery)
      state.resumeDags(dags)
    }
    val programs = dags.map { dag =>
      logger.debug(s"Activating job DAGs $dag")
      run(dag, executor)
    }

    logger.info(s"Relaunching job DAGs $dags")
    unsafeRunAsync(programs)
  }

  private[cron] def runJobsNow(dagsToRun: Set[CronDag], executor: Executor[CronScheduling])(implicit transactor: XA,
                                                                                            user: Auth.User): Unit = {
    logger.info(s"Request by ${user.userId} to run on demand DAGs ${dagsToRun.map(_.id).mkString}")
    val runNowHandlers = state.getRunNowHandlers(dagsToRun.map(_.id))
    if (runNowHandlers.isEmpty) {
      logger.info("No job in waiting state.")
    }
    for ((job, d) <- runNowHandlers) {
      logger.info(s"Running ${job.id} on demand.")
      d.complete(ScheduledAt(Instant.now, FiniteDuration(0, TimeUnit.SECONDS)) -> user).unsafeRunSync()
    }
  }

  private def run(dag: CronDag, executor: Executor[CronScheduling]): IO[Completed] = {
    def runNextPart(dag: CronDag, scheduledAt: ScheduledAt, runNowUser: Option[Auth.User]): IO[Completed] =
      // don't run anything when job is paused
      if (state.isPaused(dag)) {
        IO.pure(Completed)
      } else {
        state
          .getNextJobsInDag(dag)
          .map { job: CronJob =>
            for {
              runResult <- runAndRetry(dag, job, scheduledAt, runNowUser)
              _ <- IO(state.cronJobFinished(dag, job, success = runResult.isRight))
              result <- runNextPart(dag, scheduledAt, runNowUser)
            } yield result
          }
          .toList
          .parSequence
          .map(_ => Completed)
      }

    def runAndRetry(dag: CronDag,
                    job: CronJob,
                    scheduledAt: ScheduledAt,
                    runNowUser: Option[Auth.User],
                    retryNum: Int = 0): IO[Either[Throwable, Completed]] = {
      val runIO = for {
        runInfo <- IO {
          logger.debug(s"Sending job ${job.id} to executor")
          val cronContext = CronContext(scheduledAt.instant, runNowUser.map(_.userId))
          executor.run(job, cronContext)
        }
        _ <- IO(runNowUser.fold(())(user => runInfo._1.streams.info(s"Run now request from ${user.userId}")))
        _ <- IO(state.addNextExecutionToState(dag, runInfo._1))
        _ <- logState
        completed <- IO.fromFuture(IO(runInfo._2)).attempt
        _ <- IO(state.removeExecutionFromState(dag, runInfo._1))
        _ <- logState
      } yield completed

      runIO.flatMap {
        case Left(e) =>
          if (retryNum < job.scheduling.maxRetry) {
            val nextRetry = retryNum + 1
            logger.debug(s"Job ${job.id} has failed it's going to be retried. Retry number: $nextRetry")
            runAndRetry(dag, job, scheduledAt, runNowUser, retryNum = nextRetry)
          } else {
            logger.debug(s"Job ${job.id} has reached the maximum number of retries")
            IO.pure(Left(e))
          }
        case right => IO.pure(right)
      }
    }

    // don't run anything when dag is paused
    if (state.isPaused(dag)) {
      IO.pure(Completed)
    } else {
      val runIO = for {
        maybeScheduledAt <- IO.pure(dag.cronExpression.nextEvent())
        completed <- maybeScheduledAt match {
          // we couldn't get next event from cron4s and it didn't fail so it means we've finished for this job
          case None =>
            logger.info(s"Job DAG ${dag.id} has finished. We will not submit executions anymore")
            IO.pure(Completed)
          // we got next event: update state with it, wait for it and run it or retry until max retry
          case Some(scheduledAt) =>
            logger.debug(s"Run job DAG ${dag.id} at ${scheduledAt.instant} with a delay of ${scheduledAt.delay}")
            for {
              _ <- IO(state.addNextEventToState(dag, scheduledAt.instant))
              _ <- logState
              runNowHandler <- Deferred[IO, (ScheduledAt, Auth.User)]
              _ <- IO(state.addRunNowHandler(dag, runNowHandler))
              scheduledAtWinner <- IO.race(IO.sleep(scheduledAt.delay), runNowHandler.get)
              _ <- IO(state.removeRunNowHandler(dag))
              _ <- scheduledAtWinner match {
                //Normal sleep wakeup
                case (Left(_)) => runNextPart(dag, scheduledAt, None)
                //RunNow request by user
                case (Right(Tuple2(userScheduledAt, user))) => runNextPart(dag, userScheduledAt, Some(user))
              }
              _ <- IO(state.resetCronJobs(dag))
              completed <- run(dag, executor)
            } yield completed
        }
      } yield completed

      runIO.recover {
        case e =>
          val message = s"Fatal error Cron loop for job DAG ${dag.id} will be stopped"
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

  override def getStats(dagIds: Set[String]): Json = state.snapshotAsJson(dagIds)

  override def start(workload: Workload[CronScheduling],
                     executor: Executor[CronScheduling],
                     xa: XA,
                     logger: Logger = logger): Unit = {
    logger.info("Getting paused DAGs")
    val pausedDag = queries.getPausedJobs.transact(xa).unsafeRunSync()

    val programs = workload match {
      case CronWorkload(dags) =>
        logger.info("Init Cron State")
        state.init(dags, pausedDag)
        logger.info(s"Building IOs for Cron Workload with ${dags.size} job DAG(s)")
        dags.map(dag => run(dag, executor))
    }

    // running all jobs asynchronously
    logger.info(s"Launching Cron Workload")
    unsafeRunAsync(programs)
  }
}
