package com.criteo.cuttle.cron

import java.time.Instant
import cats.effect.concurrent.Deferred
import cats.effect.IO

import scala.concurrent.duration._
import scala.concurrent.stm.{Ref, _}
import io.circe._
import io.circe.syntax._
import io.circe.java8.time._
import com.criteo.cuttle.Auth.User
import com.criteo.cuttle.{ExecutionStatus, Job, Logger, PausedJob, Scheduling, SchedulingContext, Tag, Workload}

import scala.reflect.ClassTag

private[cron] case class ScheduledAt(instant: Instant, delay: FiniteDuration)

/**
  * The state of Cron Scheduler that allows concurrently safe mutations.
  */
private[cron] case class CronState(logger: Logger) {

  private val executions = Ref(Map.empty[CronDag, Either[Instant, Set[CronExecution]]])
  private val paused = Ref(Map.empty[CronDag, PausedJob])
  private val runNowHandlers: Ref[Map[CronDag, Deferred[IO, (ScheduledAt, User)]]] =
    Ref(Map.empty[CronDag, Deferred[IO, (ScheduledAt, User)]])(
      implicitly[ClassTag[Map[CronDag, Deferred[IO, (ScheduledAt, User)]]]]
    )
  private val jobDagState = Ref(Map.empty[CronDag, Set[(CronJob, CronJobState)]])

  private[cron] def init(availableJobDags: Set[CronDag], pausedJobs: Seq[PausedJob]) = {
    logger.debug("Cron Scheduler States initialization")

    val available = availableJobDags.map(dag => dag.id -> dag).toMap
    val initialPausedJobs = pausedJobs.collect {
      case pausedJob if available.contains(pausedJob.id) =>
        logger.debug(s"Job ${pausedJob.id} is paused")
        available(pausedJob.id) -> pausedJob
    }.toMap

    atomic { implicit txn =>
      paused() = initialPausedJobs
    }
  }

  private[cron] def getNextJobsInDag(dag: CronDag) = atomic { implicit txn =>
    val dependenciesSatisfied = dag.cronPipeline.parentsMap.filter {
      case (_, deps) =>
        deps.forall { p =>
          getFinishedJobs(dag).contains(p.parent)
        }
    }.keySet
    val candidates = dependenciesSatisfied ++ dag.cronPipeline.roots
    val results = candidates.filter { p =>
      !getRunningJobs(dag).contains(p) && !getFinishedJobs(dag).contains(p)
    }

    //update atomically the fact that the jobs are now running (to prevent another thread scheduling them)
    val jobs = jobDagState().get(dag)
    val newState: Set[(CronJob, CronJobState)] = results.map((_, Running))
    val newSet: Set[(CronJob, CronJobState)] = jobs match {
      case Some(s) => s ++ newState
      case _       => newState
    }
    jobDagState() = jobDagState() - dag
    jobDagState() = jobDagState() + (dag -> newSet)
    results
  }

  private[cron] def cronJobFinished(dag: CronDag, job: CronJob): Unit = atomic { implicit txn =>
    {
      val jobs = jobDagState().get(dag)
      if (!jobs.contains(job)) {
        val newSet: Set[(CronJob, CronJobState)] = jobs match {
          case Some(s) => s ++ Set((job, Finished))
          case _       => Set((job, Finished))
        }
        jobDagState() = jobDagState() - dag
        jobDagState() = jobDagState() + (dag -> newSet)
      }
    }
  }

  private[cron] def getRunningJobs(dag: CronDag)(implicit txn: InTxn): Set[CronJob] =
    jobDagState().get(dag).getOrElse(Set()).filter { case (_, state) => state.equals(Running) }.map { _._1 }

  private[cron] def getFinishedJobs(dag: CronDag)(implicit txn: InTxn): Set[CronJob] =
    jobDagState().get(dag).getOrElse(Set()).filter { case (_, state) => state.equals(Finished) }.map { _._1 }

  private[cron] def resetCronJobs(dag: CronDag) = atomic { implicit txn =>
    jobDagState() = jobDagState() - dag
    jobDagState() = jobDagState() + (dag -> Set())
  }

  private[cron] def getPausedJobs(): Set[PausedJob] = paused.single.get.values.toSet
  private[cron] def isPaused(dag: CronDag): Boolean = paused.single.get.contains(dag)

  private[cron] def addNextEventToState(dag: CronDag, instant: Instant): Unit = atomic { implicit txn =>
    executions() = executions() + (dag -> Left(instant))
  }

  private[cron] def addNextExecutionToState(dag: CronDag, job: CronJob, execution: CronExecution): Unit = atomic {
    implicit txn =>
      val jobParts = executions().get(dag)
      val newSet: Set[CronExecution] = jobParts match {
        case Some(Right(s)) => s ++ Set(execution)
        case _              => Set(execution)
      }
      executions() = executions() - dag
      executions() = executions() + (dag -> Right(newSet))
  }

  private[cron] def removeDagFromState(dag: CronDag): Unit = atomic { implicit txn =>
    executions() = executions() - dag
  }

  private[cron] def pauseDags(dags: Set[CronDag])(implicit user: User): Set[PausedJob] = {
    val pauseDate = Instant.now()
    atomic { implicit txn =>
      val dagsToPause = dags
        .filterNot(dag => paused().contains(dag))
        .toSeq

      dagsToPause.foreach(removeDagFromState)
      val justPausedJobs = dagsToPause.map(job => PausedJob(job.id, user, pauseDate))
      paused() = paused() ++ dagsToPause.zip(justPausedJobs)

      justPausedJobs.toSet
    }
  }

  private[cron] def resumeDags(dags: Set[CronDag]): Unit = atomic { implicit txn =>
    paused() = paused() -- dags
  }

  private[cron] def addRunNowHandler(dag: CronDag, runNowHandler: Deferred[IO, (ScheduledAt, User)]) =
    atomic { implicit txn =>
      runNowHandlers() = runNowHandlers() + (dag -> runNowHandler)
    }

  private[cron] def removeRunNowHandler(dag: CronDag) =
    atomic { implicit txn =>
      runNowHandlers() = runNowHandlers() - dag
    }

  private[cron] def getRunNowHandlers(jobIds: Set[String]) = atomic { implicit txn =>
    runNowHandlers().filter(cronJob => jobIds.contains(cronJob._1.id))
  }

  private[cron] def snapshotAsJson(jobIds: Set[String]) = atomic { implicit txn =>
    val activeJobsSnapshot = executions().collect {
      case (cronDag: CronDag, state) if jobIds.contains(cronDag.id) =>
        cronDag.asJson
          .deepMerge(
            Json.obj(
              state.fold(
                "nextInstant" -> _.asJson,
                (
                  (a: Set[CronExecution]) =>
                    ("currentExecutions" -> Json
                      .arr(a.map(_.toExecutionLog(ExecutionStatus.ExecutionRunning).asJson).toArray: _*))
                  )
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

  private[cron] def snapshot(dagIds: Set[String]) = atomic { implicit txn =>
    val activeJobsSnapshot = executions().filterKeys(cronDag => dagIds.contains(cronDag.id))
    val pausedJobsSnapshot = paused().filterKeys(cronDag => dagIds.contains(cronDag.id))

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
case class CronContext(instant: Instant, retry: Int, parentDag: String) extends SchedulingContext {

  def compareTo(other: SchedulingContext): Int = other match {
    case CronContext(otherInstant, _, _) =>
      instant.compareTo(otherInstant)
  }

  override def asJson: Json = CronContext.encoder(this)

  override def longRunningId(): String = toString
}

case object CronContext {
  implicit val encoder: Encoder[CronContext] =
    Encoder.forProduct3("interval", "retry", "parentJob")(cc => (cc.instant, cc.retry, cc.parentDag))
  implicit def decoder: Decoder[CronContext] =
    Decoder.forProduct3[CronContext, Instant, Int, String]("interval", "retry", "parentJob")(
      (instant: Instant, retry: Int, parentJob: String) => CronContext(instant, retry, parentJob)
    )
}

/** Configure a [[com.criteo.cuttle.Job job]] as a [[CronScheduling]] job.
  *
  * @param maxRetry The maximum number of retries authorized.
  * @param tz The time zone in which the cron expression is evaluated.
  */
case class CronScheduling(maxRetry: Int = 0) extends Scheduling {
  override type Context = CronContext
}

/**
  * Class regrouping cron job dags for scheduler.
  * @param dags cron dags to schedule.
  */
case class CronWorkload(dags: Set[CronDag]) extends Workload[CronScheduling] {
  override def all: Set[Job[CronScheduling]] = dags.flatMap(_.cronPipeline.vertices)
}

case class CronDag(id: String,
                   cronPipeline: CronPipeline,
                   cronExpression: CronExpression,
                   name: String = "",
                   description: String = "",
                   tags: Set[Tag] = Set.empty[Tag])

object CronDag {
  implicit val encodeUser: Encoder[CronDag] = new Encoder[CronDag] {
    override def apply(cronDag: CronDag) =
      Json.obj(
        "id" -> cronDag.id.asJson,
        "name" -> Option(cronDag.name).filterNot(_.isEmpty).asJson,
        "description" -> Option(cronDag.description).filterNot(_.isEmpty).asJson,
        "expression" -> cronDag.cronExpression.asJson,
        "tags" -> cronDag.tags.map(_.name).asJson,
        "pipeline" -> cronDag.cronPipeline.asJson
      )
  }
}

sealed trait CronJobState
case object Running extends CronJobState
case object Finished extends CronJobState
