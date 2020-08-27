package com.criteo.cuttle.cron

import java.time.Instant

import cats.effect.IO
import cats.effect.concurrent.Deferred
import com.criteo.cuttle.Auth.User
import com.criteo.cuttle.{Job, Logger, Scheduling, SchedulingContext, Tag, Workload}
import doobie.ConnectionIO
import io.circe._
import io.circe.generic.semiauto._
import io.circe.java8.time._
import io.circe.syntax._

import scala.concurrent.duration._
import scala.concurrent.stm.{atomic, Ref}
import scala.reflect.ClassTag

private[cron] case class ScheduledAt(instant: Instant, delay: FiniteDuration)

/**
  * The state of Cron Scheduler that allows concurrently safe mutations.
  */
private[cron] case class CronState(logger: Logger) {

  private val executions = Ref(Map.empty[CronDag, Either[Instant, Set[CronExecution]]])
  private val paused = Ref(Map.empty[CronDag, PausedDag])
  private val runNowHandlers: Ref[Map[CronDag, Deferred[IO, (ScheduledAt, User)]]] =
    Ref(Map.empty[CronDag, Deferred[IO, (ScheduledAt, User)]])(
      implicitly[ClassTag[Map[CronDag, Deferred[IO, (ScheduledAt, User)]]]]
    )
  private val jobDagState = Ref(Map.empty[CronDag, Map[CronJob, CronJobState]])

  private[cron] def init(availableJobDags: Set[CronDag], pausedDags: Seq[PausedDag]) = {
    logger.debug("Cron Scheduler States initialization")

    val available = availableJobDags.map(dag => dag.id -> dag).toMap
    val initialPausedDags = pausedDags.collect {
      case pausedDag if available.contains(pausedDag.id) =>
        logger.debug(s"DAG ${pausedDag.id} is paused")
        available(pausedDag.id) -> pausedDag
    }.toMap

    atomic { implicit txn =>
      paused() = initialPausedDags
    }
  }

  private[cron] def getNextJobsInDag(dag: CronDag) = atomic { implicit txn =>
    val dagState = jobDagState().getOrElse(dag, Map.empty)
    val successfulJobs = dagState.collect { case (job, Successful) => job }.toSet
    val dependenciesSatisfied = dag.cronPipeline.parentsMap.filter {
      case (_, deps) =>
        deps.forall { p =>
          successfulJobs.contains(p.from)
        }
    }.keySet
    val candidates = dependenciesSatisfied ++ dag.cronPipeline.roots
    val results = candidates -- dagState.keys

    //update atomically the fact that the jobs are now running (to prevent another thread scheduling them)
    val newState = dagState ++ results.map((_, Running))
    jobDagState() = jobDagState() + (dag -> newState)
    results
  }

  private[cron] def cronJobFinished(dag: CronDag, job: CronJob, success: Boolean): Unit = atomic { implicit txn =>
    val status = if (success) Successful else Failure
    val jobs = jobDagState().getOrElse(dag, Map.empty)
    val newState = jobs + (job -> status)
    jobDagState() = jobDagState() + (dag -> newState)
  }

  private[cron] def resetCronJobs(dag: CronDag) = atomic { implicit txn =>
    jobDagState() = jobDagState() + (dag -> Map.empty)
  }

  private[cron] def getPausedDags(): Set[PausedDag] = paused.single.get.values.toSet
  private[cron] def isPaused(dag: CronDag): Boolean = paused.single.get.contains(dag)

  private[cron] def addNextEventToState(dag: CronDag, instant: Instant): Unit = atomic { implicit txn =>
    executions() = executions() + (dag -> Left(instant))
  }

  private[cron] def addNextExecutionToState(dag: CronDag, execution: CronExecution): Unit = atomic { implicit txn =>
    val newSet: Set[CronExecution] = executions().get(dag) match {
      case Some(Right(s)) => s ++ Set(execution)
      case _              => Set(execution)
    }
    executions() = executions() + (dag -> Right(newSet))
  }

  private[cron] def removeExecutionFromState(dag: CronDag, execution: CronExecution): Unit = atomic { implicit txn =>
    executions().get(dag) match {
      case Some(Right(s)) =>
        executions() = executions() + (dag -> Right(s - execution))
      case _ =>
    }
  }

  private[cron] def removeDagFromState(dag: CronDag): Unit = atomic { implicit txn =>
    executions() = executions() - dag
  }

  private[cron] def pauseDags(dags: Set[CronDag])(implicit user: User): Set[PausedDag] = {
    val pauseDate = Instant.now()
    atomic { implicit txn =>
      val dagsToPause = dags
        .filterNot(dag => paused().contains(dag))
        .toSeq

      dagsToPause.foreach(removeDagFromState)
      val justPausedDags = dagsToPause.map(dag => PausedDag(dag.id, user, pauseDate))
      paused() = paused() ++ dagsToPause.zip(justPausedDags)

      justPausedDags.toSet
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

  private[cron] def getRunNowHandlers(dagIds: Set[String]) = atomic { implicit txn =>
    runNowHandlers().filter(cronJob => dagIds.contains(cronJob._1.id))
  }

  private[cron] def snapshotAsJson(dagIds: Set[String]) = atomic { implicit txn =>
    val activeDagsSnapshot = executions().collect {
      case (cronDag: CronDag, Left(nextInstant)) if dagIds.contains(cronDag.id) =>
        Json.obj(
          "id" -> cronDag.id.asJson,
          "status" -> "waiting".asJson,
          "nextInstant" -> nextInstant.asJson
        )
      case (cronDag: CronDag, Right(_)) if dagIds.contains(cronDag.id) =>
        Json.obj(
          "id" -> cronDag.id.asJson,
          "status" -> "running".asJson
        )
    }
    Json.arr(
      activeDagsSnapshot.toSeq: _*
    )
  }

  private[cron] def snapshot(dagIds: Set[String]) = atomic { implicit txn =>
    val activeJobsSnapshot = executions().filterKeys(cronDag => dagIds.contains(cronDag.id))
    val pausedDagsSnapshot = paused().filterKeys(cronDag => dagIds.contains(cronDag.id))

    activeJobsSnapshot -> pausedDagsSnapshot
  }

  override def toString: String = {
    val builder = new StringBuilder()
    val state = executions.single.get
    builder.append("\n======State======\n")
    state.foreach {
      case (dag, dagState) =>
        val messages = Seq(
          dag.id,
          dagState.fold(_.toString, _.map(_.id).mkString(","))
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
case class CronContext(instant: Instant, runNowUser: Option[String] = None) extends SchedulingContext {

  def compareTo(other: SchedulingContext): Int = other match {
    case CronContext(otherInstant, _) =>
      instant.compareTo(otherInstant)
  }

  override def logIntoDatabase: ConnectionIO[String] = Database.serializeContext(this)

  override def asJson: Json = CronContext.encoder(this)

  override def longRunningId(): String = s"$instant|${runNowUser.getOrElse("")}"
}

case object CronContext {
  implicit val encoder: Encoder[CronContext] = deriveEncoder
  implicit def decoder: Decoder[CronContext] = deriveDecoder
}

/** Configure a [[com.criteo.cuttle.Job job]] as a [[CronScheduling]] job.
  *
  * @param maxRetry The maximum number of retries authorized.
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
case object Successful extends CronJobState
case object Failure extends CronJobState
