package com.criteo.cuttle

import scala.concurrent.Future

import cats.Eq

/**
  * The workflow to be run by cuttle. A workflow is defined for a given [[Scheduling]],
  * for example it can be a [[timeseries.TimeSeries TimeSeries]] workflow.
  *
  * @tparam S The kind of [[Scheduling]] used by this workflow.
  **/
trait Workflow[S <: Scheduling] {
  private[criteo] type Dependency = (Job[S], Job[S], S#DependencyDescriptor)

  private[criteo] def vertices: Set[Job[S]]
  private[criteo] def edges: Set[Dependency]

  private[cuttle] def roots: Set[Job[S]] = {
    val childNodes = edges.map { case (child, _, _) => child }
    vertices.filter(!childNodes.contains(_))
  }

  private[cuttle] def leaves: Set[Job[S]] = {
    val parentNodes = edges.map { case (_, parent, _) => parent }
    vertices.filter(!parentNodes.contains(_))
  }

  // Returns a list of jobs in the workflow sorted topologically, using Kahn's algorithm. At the
  // same time checks that there is no cycle.
  private[cuttle] lazy val jobsInOrder: List[Job[S]] = graph.topologicalSort[Job[S]](
    vertices,
    edges.map { case (child, parent, _) => parent -> child }
  ) match {
    case Some(sortedNodes) => sortedNodes
    case None => throw new IllegalArgumentException("Workflow has at least one cycle")
  }

  /**
    * Compose a [[Workflow]] with another [[Workflow]] but without any
    * dependency. It won't add any edge to the graph.
    *
    * @param otherWorflow The workflow to compose this workflow with.
    */
  def and(otherWorflow: Workflow[S]): Workflow[S] = {
    val leftWorkflow = this
    new Workflow[S] {
      val vertices = leftWorkflow.vertices ++ otherWorflow.vertices
      val edges = leftWorkflow.edges ++ otherWorflow.edges
    }
  }

  /**
    * Compose a [[Workflow]] with a second [[Workflow]] with a dependencies added between
    * all this workflow roots and the other workflow leaves. The added dependencies will use the
    * default dependency descriptors implicitly provided by the [[Scheduling]] used by this workflow.
    *
    * @param rightWorkflow The workflow to compose this workflow with.
    * @param dependencyDescriptor If injected implicitly, default dependency descriptor for the current [[Scheduling]].
    */
  def dependsOn(rightWorkflow: Workflow[S])(implicit dependencyDescriptor: S#DependencyDescriptor): Workflow[S] =
    dependsOn((rightWorkflow, dependencyDescriptor))

  /**
    * Compose a [[Workflow]] with a second [[Workflow]] with a dependencies added between
    * all this workflow roots and the other workflow leaves. The added dependencies will use the
    * specified dependency descriptors.
    *
    * @param rightWorkflow The workflow to compose this workflow with.
    * @param dependencyDescriptor The dependency descriptor to use for the newly created dependency edges.
    */
  def dependsOn(rightOperand: (Workflow[S], S#DependencyDescriptor)): Workflow[S] = {
    val (rightWorkflow, depDescriptor) = rightOperand
    val leftWorkflow = this
    val newEdges: Set[Dependency] = for {
      v1 <- leftWorkflow.roots
      v2 <- rightWorkflow.leaves
    } yield (v1, v2, depDescriptor)
    new Workflow[S] {
      val vertices = leftWorkflow.vertices ++ rightWorkflow.vertices
      val edges = leftWorkflow.edges ++ rightWorkflow.edges ++ newEdges
    }
  }
}

/** Utilities for [[Workflow]]. */
object Workflow {

  /** An empty [[Workflow]] (empty graph). */
  def empty[S <: Scheduling]: Workflow[S] = new Workflow[S] {
    def vertices = Set.empty
    def edges = Set.empty
  }

  /**
    * Validation of:
    * - absence of cycles in the workflow
    * - absence of jobs with the same id
    *  @return the list of errors in the workflow, if any
    */
  def validate[S <: Scheduling](workflow: Workflow[S]): List[String] = {
    val errors = collection.mutable.ListBuffer.empty[String]

    if(graph.topologicalSort[Job[S]](
      workflow.vertices,
      workflow.edges.map { case (child, parent, _) => parent -> child }
    ).isEmpty) {
      errors += "Workflow has at least one cycle"
    }

    workflow.vertices.groupBy(_.id).collect {
      case (id: String, jobs) if jobs.size > 1 => id
    } foreach (id => errors += s"Id $id is used by more than 1 job")

    errors.toList
  }
}

/** Allow to tag a job. Tags can be used in the UI/API to filter jobs
  * and more easily retrieve them.
  *
  * @param name Tag name as displayed in the UI.
  * @param description Description as displayed in the UI.
  */
case class Tag(name: String, description: String = "")

/** The job [[SideEffect]] is the most important part as it represents the real
  * job logic to execute. A job is defined for a given [[Scheduling]],
  * for example it can be a [[timeseries.TimeSeries TimeSeries]] job. Jobs are also [[Workflow]] with a
  * single vertice.
  *
  * @tparam S The kind of [[Scheduling]] used by this job.
  * @param id the internal job id. It will be sued to track the job state in the database, so it must not
  *           change over time otherwise the job will be seen as a new one by the scheduler.
  * @param scheduling The scheduling configuration for the job. For example a [[timeseries.TimeSeries TimeSeries]] job can
  *                   be configured to be hourly or daily, etc.
  * @param name The job name as displayed in the UI.
  * @param description The job description as displayed in the UI.
  * @param tags The job tags used to filter jobs in the UI.
  * @param effect The job side effect, representing the real job execution.
  */
case class Job[S <: Scheduling](id: String,
                                scheduling: S,
                                name: String = "",
                                description: String = "",
                                tags: Set[Tag] = Set.empty[Tag])(val effect: SideEffect[S])
    extends Workflow[S] {
  private[criteo] val vertices = Set(this)
  private[criteo] val edges = Set.empty[Dependency]

  /** Run this job for the given [[Execution]].
    *
    * @param execution The execution instance.
    * @return A future indicating the execution result (Failed future means failed execution).
    */
  private[cuttle] def run(execution: Execution[S]): Future[Completed] = effect(execution)
}

case object Job {
  implicit def eqInstance[S <: Scheduling] = Eq.fromUniversalEquals[Job[S]]
}