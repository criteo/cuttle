package com.criteo.cuttle

import lol.http.PartialService
import io.circe.Json
import doobie.imports._
import java.util.Comparator

import Metrics.MetricProvider
import Auth._

/** A scheduler interpret a [[Workflow]] and instanciate [[Execution Executions]] for all
  * defined [[Job Jobs]]. For example, a typical cuttle [[Scheduler]] is the [[timeseries.TimeSeries TimeSeries]]
  * scheduler that executes the graph for each time partition.
  *
  * @tparam S The king of [[Scheduling]] managed by this [[Scheduler]].
  */
trait Scheduler[S <: Scheduling] extends MetricProvider {

  /** Starts the scheduler for the given [[Workflow]]. Immediatly the scheduler will start interpreting
    * the workflow and generate [[Execution Executions]] sent to the provided [[Executor]].
    *
    * @param workflow The workflow to run.
    * @param executor The executor to use to run the generated [[Execution Executions]].
    * @param xa The doobie transactor to use to persist the scheduler state if needed.
    * @param logger The logger to use to log internal debug state if neeed.
    */
  def start(workflow: Workflow[S], executor: Executor[S], xa: XA, logger: Logger): Unit

  private[cuttle] def publicRoutes(workflow: Workflow[S], executor: Executor[S], xa: XA): PartialService =
    PartialFunction.empty
  private[cuttle] def privateRoutes(workflow: Workflow[S], executor: Executor[S], xa: XA): AuthenticatedService =
    PartialFunction.empty

  /** Provide a doobie SQL `Fragment` used to retrieve all execution contexts from
    * the execution logs.
    */
  val allContexts: Fragment

  /** Returns a Json object containing the internal scheduler statistics informations.
    * These data will be send to the UI and can be used by the scheduler UI if needed.
    *
    * @param jobs Fiter the statistics for the provided list of jobs.
    */
  def getStats(jobs: Set[String]): Json
}

/** A scheduling context is the input given to each [[Execution]] that will be created
  * for a given [[Scheduling]]. For example, for a [[timeseries.TimeSeries TimeSeries]] scheduling the context
  * contain the time partition for which the job is running. */
trait SchedulingContext {

  /** Serialize the context information to JSON. Mainly used for the UI. */
  def toJson: Json

  /** The doobie effect needed to serialize the context to the database. */
  def log: ConnectionIO[String]

  /** Compare to another context. In the current design only context of the same types will be
    * compared to each other because a workflow/project is defined for a single [[Scheduling]] type. */
  def compareTo(other: SchedulingContext): Int
}

/** Utilities for [[SchedulingContext]] */
object SchedulingContext {

  /** Provide an implicit `Ordering` for [[SchedulingContext]] based on the `compareTo` function. */
  implicit val ordering: Ordering[SchedulingContext] =
    Ordering.comparatorToOrdering(new Comparator[SchedulingContext] {
      def compare(o1: SchedulingContext, o2: SchedulingContext) = o1.compareTo(o2)
    })
}

/** Represent a type of scheduling. A typical cuttle scheduling is [[timeseries.TimeSeries TimeSeries]] for example, that
  * is a scheduling based on a calendar. */
trait Scheduling {

  /** The [[SchedulingContext]] type that will be passed to [[Execution]] created by
    * the [[Scheduler]] managing this [[Scheduling]]. */
  type Context <: SchedulingContext

  /** The [[DependencyDescriptor]] used to annotate the edges of the [[Workflow]] graphs created
    * for this [[Scheduling]]. For example a [[timeseries.TimeSeries TimeSeries]] scheduling provide a configurable time
    * offset, allowing 2 jobs to depends of each other with a specified offset (like hourly job A depends on
    * hourly job B with a 3 hours time offset). */
  type DependencyDescriptor

  /** Output the configuration details of this [[Scheduling]] as Json.
    * Used for the UI. */
  def toJson: Json
}
