package com.criteo.cuttle.monotonic

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.criteo.cuttle.{Executor, Job, Scheduler, Scheduling, SchedulingContext, Workflow, XA, utils}
import doobie.imports._
import io.circe.{Encoder, Json}

import scala.concurrent.Future
import scala.concurrent.duration.{Duration => ScalaDuration}
import scala.concurrent.stm._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.ClassTag

object MonotonicContext {
  def apply[T: Encoder: ClassTag](execParams: T, execTime: LocalDateTime) = new MonotonicContext[T] {
    def executionParams = execParams

    def executionTime = execTime

    val enc = implicitly[Encoder[T]]

    override def toJson: Json = Json.obj(
      "exec_params" -> enc(execParams),
      "exec_time" -> Json.fromString(DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(executionTime))
    )

    override def log: ConnectionIO[String] = Database.serializeContext(this)

    override def compareTo(other: SchedulingContext): Int = other match {
      case o: MonotonicContext[T] => ordering.compare(this, o)
    }
  }

  implicit def ordering[T] = Ordering.fromLessThan((t1: MonotonicContext[T], t2: MonotonicContext[T]) => t1.executionTime.isBefore(t2.executionTime))
}

trait MonotonicContext[T] extends SchedulingContext {
  /**
    * Parameters of jobs.
    * Each graph execution will provide different values.
    *
    * @return
    */
  def executionParams: T

  /**
    * Workflow execution time, used to order executions.
    * This is the time of the completion of the trigger.
    *
    * @return graph execution time
    */
  def executionTime: LocalDateTime

  def enc: Encoder[T]
}

class TriggerJob[T](triggerName: String, trigger: () => Future[T], mon: Monotonic[T])
  extends Job[Monotonic[T]](id = triggerName, name = "trigger_job", description = "Triggers the execution of a workflow", tags = Set.empty, scheduling = mon)(_ => Future.successful(())) {
  def apply() = trigger()
}

case class Monotonic[T: ClassTag: Encoder]() extends Scheduling {
  type Context = MonotonicContext[T]
  type DependencyDescriptor = MonotonicDependency

  override def toJson: Json = Json.fromString("")
}

object Monotonic {
  type Aux[T] = () => Future[T]
}

// TODO: maybe use a partial representation of the context to materialize timeouts, partial executions or filters
// Example: half of the IDs were processed only from job A to job B
trait MonotonicDependency

case object MonotonicDependency extends MonotonicDependency

class MonotonicScheduler[T: ClassTag: Encoder](maxWorkflowParallelRuns: Int = 1)(implicit val trigger: Monotonic.Aux[T])
  extends Scheduler[Monotonic[T]] {

  type MonotonicJob = Job[Monotonic[T]]
  // State is defined as a list of successful executions for each job
  // We do not keep track of failed job in the state, they are listed as not executed
  type State = Map[MonotonicContext[T], Set[MonotonicJob]]
  val state = TMap.empty[MonotonicContext[T], Set[MonotonicJob]]

  val triggerJob = new TriggerJob("event", trigger, new Monotonic[T])

  def addCompletedRuns(finishedExecutions: Set[(MonotonicJob, MonotonicContext[T])])
                      (implicit txn: InTxn) =
    finishedExecutions.foreach { case (job, context) =>
      state.update(context,
        state.get(context).getOrElse(Set.empty) + job)
    }



  def start(graph: Workflow[Monotonic[T]], executor: Executor[Monotonic[T]], xa: XA) = {
    Database.doSchemaUpdates.transact(xa).unsafePerformIO

    def runTrigger(): Unit =
      trigger() onSuccess { case res =>
        atomic { implicit txn =>
          state += MonotonicContext[T](res, LocalDateTime.now) -> Set(triggerJob)
        }
        runTrigger()
      }

    def go(running: Set[(MonotonicJob, MonotonicContext[T], Future[Unit])]): Unit = {
      val (done, stillRunning) = running.partition { case (_, _, futureExec) => futureExec.isCompleted }
      val stateSnapshot = atomic { implicit txn =>
        addCompletedRuns(done.map { case (job, ctx, _) => (job, ctx) })
        state.snapshot
      }

      val toRun = next(
        graph,
        stateSnapshot,
        running
      )
      val newRunning = stillRunning ++ executor.runAll(toRun.toSeq).map {
        case (execution, result) => (execution.job, execution.context, result)
      }
      Future.firstCompletedOf(utils.Timeout(ScalaDuration.create(1, "s")) :: newRunning.map(_._3).toList).
        andThen { case _ => go(newRunning) }
    }
    go(Set.empty)

    def next(graph: Workflow[Monotonic[T]], currentState: State, running: Set[(MonotonicJob, MonotonicContext[T], Future[Unit])])
    : Set[(MonotonicJob, MonotonicContext[T])] = {
      // Get graph runs by execution time
      val runningWorkflowPerExecTime = running
        .map { case (job, context, future) => (job, context) }
        .groupBy { case (job, context) => context.executionTime }
        .mapValues(_.toMap)

      if (runningWorkflowPerExecTime.size < maxWorkflowParallelRuns) {
        // Last executions (sorted by key => batch exec time)
        val lastWorkflows: Map[MonotonicContext[T], Set[MonotonicJob]] = currentState.toSeq.sortBy(_._1).takeRight(maxWorkflowParallelRuns).toMap
        val completeWorkflow = graph dependsOn triggerJob


        // For each vertex check if we should run jobLeft
        for {
          (childJob, parentJob, _) <- completeWorkflow.edges
          (context, lastExecutions) <- lastWorkflows
          job <- lastExecutions
          if (job == parentJob) &&
            runningWorkflowPerExecTime.get(context.executionTime).forall(!_.contains(childJob)) &&
            !currentState(context).contains(childJob)
        } yield childJob -> context
      }
      else
        Set.empty
    }

    runTrigger()
  }

  override val allContexts: Fragment = Database.sqlGetContexts()
}