package com.criteo.cuttle.cron

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant, ZoneId, ZoneOffset}

import cats.effect.concurrent.Deferred
import cats.effect.IO

import scala.concurrent.duration._
import scala.concurrent.stm.{Ref, _}
import cron4s.{Cron, _}
import cron4s.lib.javatime._
import io.circe._
import io.circe.syntax._
import io.circe.java8.time._
import com.criteo.cuttle.Auth.User
import com.criteo.cuttle.{ExecutionStatus, Job, Logger, PausedJob, Scheduling, SchedulingContext, Workload}

import scala.reflect.ClassTag

private[cron] case class ScheduledAt(instant: Instant, delay: FiniteDuration)

/**
  * The state of Cron Scheduler that allows concurrently safe mutations.
  */
private[cron] case class CronState(logger: Logger) {
  private val executions = Ref(Map.empty[CronJob, Either[Instant, Set[CronExecution]]])
  private val paused = Ref(Map.empty[CronJob, PausedJob])
  private val runNowHandlers: Ref[Map[CronJob, Deferred[IO, (ScheduledAt, User)]]] =
    Ref(Map.empty[CronJob, Deferred[IO, (ScheduledAt, User)]])(
       implicitly[ClassTag[Map[CronJob, Deferred[IO, (ScheduledAt, User)]]]]
    )
  private val cronJobParts = Ref(Map.empty[CronJob, Set[(CronJobPart, CronJobPartState)]])

  private[cron] def init(availableJobs: Set[CronJob], pausedJobs: Seq[PausedJob]) = {
    logger.debug("Cron Scheduler States initialization")
    val available = availableJobs.map(job => job.id -> job).toMap
    val initialPausedJobs = pausedJobs.collect {
      case pausedJob if available.contains(pausedJob.id) =>
        logger.debug(s"Job ${pausedJob.id} is paused")
        available(pausedJob.id) -> pausedJob
    }.toMap

    atomic { implicit txn =>
      paused() = initialPausedJobs
    }
  }

  private[cron] def getNextParts(job: CronJob) = atomic { implicit txn =>
    val dependenciesSatisfied = job.pipeline.parentsMap.filter {
      case (_, deps) => deps.forall { p => getFinishedJobParts(job).contains(p.parent)}
    }.keySet
    val candidates = dependenciesSatisfied ++ job.pipeline.roots
    val results = candidates.filter{p => !getRunningJobParts(job).contains(p) && !getFinishedJobParts(job).contains(p)}

    //update atomically the fact that the job parts are now running (to prevent another thread getting them)
    val jobParts = cronJobParts().get(job)
    val newState: Set[(CronJobPart, CronJobPartState)] = results.map((_, Running))
    val newSet: Set[(CronJobPart, CronJobPartState)] = jobParts match {
      case Some(s) => s ++ newState
      case _       => newState
    }
    cronJobParts() = cronJobParts() - job
    cronJobParts() = cronJobParts() + (job -> newSet)
    results
  }

  private[cron] def cronJobPartFinished(job: CronJob, jobPart: CronJobPart): Unit = atomic { implicit txn =>
    {
      val jobParts = cronJobParts().get(job)
      if (jobParts.contains(jobPart)) {
        //there may be more than one thread that get the same job part, only one should win.
        throw new RuntimeException("Job part already started")
      }
      val newSet: Set[(CronJobPart, CronJobPartState)] = jobParts match {
        case Some(s) => s ++ Set((jobPart, Finished))
        case _       => Set((jobPart, Finished))
      }
      cronJobParts() = cronJobParts() - job
      cronJobParts() = cronJobParts() + (job -> newSet)
    }
  }

  private[cron] def getRunningJobParts(job: CronJob)(implicit txn: InTxn): Set[CronJobPart] =
    cronJobParts().get(job).getOrElse(Set()).filter{case (_,state) => state.equals(Running)}.map{_._1}

  private[cron] def getFinishedJobParts(job: CronJob)(implicit txn: InTxn): Set[CronJobPart] =
    cronJobParts().get(job).getOrElse(Set()).filter{case (_,state) => state.equals(Finished)}.map{_._1}

  private[cron] def resetCronJobParts(job: CronJob) = atomic { implicit txn =>
    cronJobParts() = cronJobParts() - job
    cronJobParts() = cronJobParts() + (job -> Set())
  }

  private[cron] def getPausedJobs(): Set[PausedJob] = paused.single.get.values.toSet
  private[cron] def isPaused(job: CronJob): Boolean = paused.single.get.contains(job)

  private[cron] def addNextEventToState(job: CronJob, instant: Instant): Unit = atomic { implicit txn =>
    executions() = executions() + (job -> Left(instant))
  }

  private[cron] def addNextExecutionToState(job: CronJob, jobPart: CronJobPart, execution: CronExecution): Unit = atomic { implicit txn =>
    val jobParts = executions().get(job)
    val newSet: Set[CronExecution] = jobParts match {
      case Some(Right(s)) => s ++ Set(execution)
      case _              => Set(execution)
    }
    executions() = executions() - job
    executions() = executions() + (job -> Right(newSet))
  }


  private[cron] def removeJobFromState(job: CronJob): Unit = atomic { implicit txn =>
    executions() = executions() - job
  }

  private[cron] def pauseJobs(jobs: Set[CronJob])(implicit user: User): Set[PausedJob] = {
    val pauseDate = Instant.now()
    atomic { implicit txn =>
      val jobsToPause = jobs
        .filterNot(job => paused().contains(job))
        .toSeq

      jobsToPause.foreach(removeJobFromState)
      val justPausedJobs = jobsToPause.map(job => PausedJob(job.id, user, pauseDate))
      paused() = paused() ++ jobsToPause.zip(justPausedJobs)

      justPausedJobs.toSet
    }
  }

  private[cron] def resumeJobs(jobs: Set[CronJob]): Unit = atomic { implicit txn =>
    paused() = paused() -- jobs
  }

  private[cron] def addRunNowHandler(job: CronJob, runNowHandler: Deferred[IO, (ScheduledAt, User)]) =
    atomic { implicit txn =>
      runNowHandlers() = runNowHandlers() + (job -> runNowHandler)
    }

  private[cron] def removeRunNowHandler(job: CronJob) =
    atomic { implicit txn =>
      runNowHandlers() = runNowHandlers() - job
    }

  private[cron] def getRunNowHandlers(jobIds: Set[String]) = atomic { implicit txn =>
    runNowHandlers().filter(cronJob => jobIds.contains(cronJob._1.id))
  }

  private[cron] def snapshotAsJson(jobIds: Set[String]) = atomic { implicit txn =>
    val activeJobsSnapshot = executions().collect {
      case (cronJob: CronJob, state) if jobIds.contains(cronJob.id) =>
        cronJob.asJson
          .deepMerge(
            Json.obj(
              state.fold(
                "nextInstant" -> _.asJson,
                ((a: Set[CronExecution]) => ("currentExecutions" -> Json.arr(a.map(_.toExecutionLog(ExecutionStatus.ExecutionRunning).asJson).toArray:_*)))
              )
            )
          )
          .deepMerge(
            Json.obj(
              "status" -> "active".asJson
            )
          )
    }
    val pausedJobsSnapshot = paused().collect {
      case (cronJob, pausedJob) if jobIds.contains(cronJob.id) => pausedJob.asJson
    }
    val acc = (activeJobsSnapshot ++ pausedJobsSnapshot).toSeq
    Json.arr(
      acc: _*
    )
  }

  private[cron] def snapshot(jobIds: Set[String]) = atomic { implicit txn =>
    val activeJobsSnapshot = executions().filterKeys(cronJob => jobIds.contains(cronJob.id))
    val pausedJobsSnapshot = paused().filterKeys(cronJob => jobIds.contains(cronJob.id))

    activeJobsSnapshot -> pausedJobsSnapshot
  }

  override def toString(): String = {
    val builder = new StringBuilder()
    val state = executions.single.get
    builder.append("\n======State======\n")
    state.foreach {
      case (job, jobState) =>
        val messages = Seq(
          job.id,
          jobState.fold(_ toString, _.map(_.id).mkString(","))
        )
        builder.append(messages mkString " :: ")
        builder.append("\n")
    }
    builder.append("======End State======")
    builder.toString()
  }
}

/** A [[CronContext]] is passed to [[com.criteo.cuttle.Execution executions]] initiated by
  * the [[CronScheduler]].
  */
case class CronContext(instant: Instant, retryNum: Int, parentJob: String) extends SchedulingContext {
  val retry: Int = retryNum

  def compareTo(other: SchedulingContext): Int = other match {
    case CronContext(otherInstant, _, _) =>
      instant.compareTo(otherInstant)
  }

  override def asJson: Json = CronContext.encoder(this)

  override def longRunningId(): String = toString
}

case object CronContext {
  implicit val encoder: Encoder[CronContext] = Encoder.forProduct3("interval", "retry", "parentJob")(cc => (cc.instant, cc.retry, cc.parentJob))
  implicit def decoder: Decoder[CronContext] =
    Decoder.forProduct3[CronContext, Instant, Int, String]("interval", "retry", "parentJob")(
      (instant: Instant, retry: Int, parentJob: String) => CronContext(instant, retry, parentJob)
    )
}

/** Configure a [[com.criteo.cuttle.Job job]] as a [[CronScheduling]] job.
  *
  * @param cronExpression Cron expression to be parsed by https://github.com/alonsodomin/cron4s.
  *                       See the link above for more details.
  * @param maxRetry The maximum number of retries authorized.
  * @param tz The time zone in which the cron expression is evaluated.
  */
case class CronScheduling(cronExpression: String, maxRetry: Int, tz: ZoneId = ZoneOffset.UTC) extends Scheduling {
  override type Context = CronContext
  // https://www.baeldung.com/cron-expressions
  // https://www.freeformatter.com/cron-expression-generator-quartz.html
  private val cronExpr = Cron.unsafeParse(cronExpression)

  private def toZonedDateTime(instant: Instant) =
    instant.atZone(tz)

  def nextEvent(): Option[ScheduledAt] = {
    val instant = Instant.now()
    cronExpr.next(toZonedDateTime(instant)).map { next =>
      // add 1 second as between doesn't include the end of the interval
      val delay = Duration.between(instant, next).get(ChronoUnit.SECONDS).seconds.plus(1.second)
      ScheduledAt(next.toInstant, delay)
    }
  }
}

/**
  * Class regrouping jobs for scheduler. It doesn't imply any order.
  * @param jobs Jobs to schedule.
  */
case class CronWorkload(cronJobs: Set[CronJob]) extends Workload[CronScheduling] {
  override def all: Set[Job[CronScheduling]] = cronJobs.map(CronJob.cronJobToJob(_))
}

sealed trait CronJobPartState
case object Running extends CronJobPartState
case object Finished extends CronJobPartState






