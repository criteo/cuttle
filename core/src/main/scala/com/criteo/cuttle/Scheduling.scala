package com.criteo.cuttle

import io.circe.Json
import doobie.imports._
import cats.free._
import java.util.Comparator

import Metrics.MetricProvider

/** A scheduler interpret a [[Workflow]] and instanciate [[Execution Executions]] for all
  * defined [[Job Jobs]]. For example, a typical cuttle [[Scheduler]] is the [[timeseries.TimeSeries TimeSeries]]
  * scheduler that executes the graph for each time partition.
  *
  * @tparam S The king of [[Scheduling]] managed by this [[Scheduler]].
  */
trait Scheduler[S <: Scheduling] extends MetricProvider[S] {
  def name: String = this.getClass.getSimpleName

  /** Starts the scheduler for the given [[Workflow]]. Immediatly the scheduler will start interpreting
    * the workflow and generate [[Execution Executions]] sent to the provided [[Executor]].
    *
    * @param jobs The jobs to run.
    * @param executor The executor to use to run the generated [[Execution Executions]].
    * @param xa The doobie transactor to use to persist the scheduler state if needed.
    * @param logger The logger to use to log internal debug state if neeed.
    */
  def start(jobs: Workload[S], executor: Executor[S], xa: XA, logger: Logger): Unit

  /** Provide a doobie SQL `Fragment` used to retrieve all execution contexts from
    * the execution logs.
    *
    * This fragment must be a SELECT query returning (id,json) tuples for all known
    * scheduling contexts.
    */
  val allContexts: Fragment = Queries.getAllContexts

  /** Returns a Json object containing the internal scheduler statistics informations.
    * These data will be send to the UI and can be used by the scheduler UI if needed.
    *
    * @param jobs Filter the statistics for the provided list of jobs.
    */
  def getStats(jobs: Set[String]): Json = Json.obj()
}

/** A scheduling context is the input given to each [[Execution]] that will be created
  * for a given [[Scheduling]]. For example, for a [[timeseries.TimeSeries TimeSeries]] scheduling the context
  * contain the time partition for which the job is running. */
trait SchedulingContext {

  /** Serialize the context information to JSON. */
  def asJson: Json = Json.fromString(toString)

  /** The doobie effect needed to serialize the context to the database. */
  def logIntoDatabase: ConnectionIO[String] = Free.pure(asJson.toString)

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

  /** Output the configuration details of this [[Scheduling]] as Json. */
  def asJson: Json = Json.fromString(toString)
}
