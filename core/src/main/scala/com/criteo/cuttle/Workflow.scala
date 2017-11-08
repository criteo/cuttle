package com.criteo.cuttle

import scala.concurrent.{Future}

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

  // Kahn's algorithm
  // We construct the best linear representation for our DAG. At the
  // same time it checks that there is no cycle.
  private[cuttle] lazy val jobsInOrder: List[Job[S]] = {
    val result = collection.mutable.ListBuffer.empty[Job[S]]
    val edges = collection.mutable.Set(this.edges.toSeq: _*)
    val orderedRoots = collection.mutable.SortedSet(roots.toSeq: _*)(Ordering.by(_.id))
    while (orderedRoots.nonEmpty) {
      val root = orderedRoots.head
      orderedRoots.remove(root)
      result.append(root)
      val edgesToRemove = edges.filter(_._2 == root)

      edgesToRemove.foreach {
        case edge @ (child, _, _) =>
          edges.remove(edge)
          if (!edges.exists(_._1 == child)) {
            orderedRoots.add(child)
          }
      }
    }

    require(edges.isEmpty, "Workflow has at least one cycle")
    result.toList
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

  private[cuttle] lazy val roots = vertices.filter(v => edges.forall { case (v1, _, _)  => v1 != v })
  private[cuttle] lazy val leaves = vertices.filter(v => edges.forall { case (_, v2, _) => v2 != v })

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
