package com.criteo.cuttle.timeseries

import io.circe._
import io.circe.syntax._

import com.criteo.cuttle._

/**
  * A timeseries workflow
  **/
trait Workflow extends Workload[TimeSeries] {

  implicit def workflowEncoder =
    new Encoder[Workflow] {
      override def apply(workflow: Workflow) = {
        val jobs = workflow.jobsInOrder.asJson
        val tags = workflow.vertices.flatMap(_.tags).asJson
        val dependencies = workflow.edges.map {
          case (to, from, _) =>
            Json.obj(
              "from" -> from.id.asJson,
              "to" -> to.id.asJson
            )
        }.asJson
        Json.obj(
          "jobs" -> jobs,
          "dependencies" -> dependencies,
          "tags" -> tags
        )
      }
    }
  def all = vertices
  override def asJson = workflowEncoder(this)

  private[criteo] type Dependency = (Job[TimeSeries], Job[TimeSeries], TimeSeriesDependency)

  private[criteo] def vertices: Set[Job[TimeSeries]]
  private[criteo] def edges: Set[Dependency]

  private[cuttle] def roots: Set[Job[TimeSeries]] = {
    val childNodes = edges.map { case (child, _, _) => child }
    vertices.filter(!childNodes.contains(_))
  }

  private[cuttle] def leaves: Set[Job[TimeSeries]] = {
    val parentNodes = edges.map { case (_, parent, _) => parent }
    vertices.filter(!parentNodes.contains(_))
  }

  // Returns a list of jobs in the workflow sorted topologically, using Kahn's algorithm. At the
  // same time checks that there is no cycle.
  private[cuttle] lazy val jobsInOrder: List[Job[TimeSeries]] = graph.topologicalSort[Job[TimeSeries]](
    vertices,
    edges.map { case (child, parent, _) => parent -> child }
  ) match {
    case Some(sortedNodes) => sortedNodes
    case None              => throw new IllegalArgumentException("Workflow has at least one cycle")
  }

  /**
    * Compose a [[Workflow]] with another [[Workflow]] but without any
    * dependency. It won't add any edge to the graph.
    *
    * @param otherWorflow The workflow to compose this workflow with.
    */
  def and(otherWorflow: Workflow): Workflow = {
    val leftWorkflow = this
    new Workflow {
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
  def dependsOn(rightWorkflow: Workflow)(implicit dependencyDescriptor: TimeSeriesDependency): Workflow =
    dependsOn((rightWorkflow, dependencyDescriptor))

  /**
    * Compose a [[Workflow]] with a second [[Workflow]] with a dependencies added between
    * all this workflow roots and the other workflow leaves. The added dependencies will use the
    * specified dependency descriptors.
    *
    * @param rightOperand The workflow to compose this workflow with.
    */
  def dependsOn(rightOperand: (Workflow, TimeSeriesDependency)): Workflow = {
    val (rightWorkflow, depDescriptor) = rightOperand
    val leftWorkflow = this
    val newEdges: Set[Dependency] = for {
      v1 <- leftWorkflow.roots
      v2 <- rightWorkflow.leaves
    } yield (v1, v2, depDescriptor)
    new Workflow {
      val vertices = leftWorkflow.vertices ++ rightWorkflow.vertices
      val edges = leftWorkflow.edges ++ rightWorkflow.edges ++ newEdges
    }
  }
}

/** Utilities for [[Workflow]]. */
object Workflow {

  /** An empty [[Workflow]] (empty graph). */
  def empty[S <: Scheduling]: Workflow = new Workflow {
    def vertices = Set.empty
    def edges = Set.empty
  }

  /**
    * Validation of:
    * - absence of cycles in the workflow
    * - absence of jobs with the same id
    *  @return the list of errors in the workflow, if any
    */
  def validate(workflow: Workflow): List[String] = {
    val errors = collection.mutable.ListBuffer.empty[String]

    if (graph
          .topologicalSort[Job[TimeSeries]](
            workflow.vertices,
            workflow.edges.map { case (child, parent, _) => parent -> child }
          )
          .isEmpty) {
      errors += "Workflow has at least one cycle"
    }

    graph
      .findStronglyConnectedComponents[Job[TimeSeries]](
        workflow.vertices,
        workflow.edges.map { case (child, parent, _) => parent -> child }
      )
      .filter(scc => scc.size >= 2) // Strongly connected components with more than 2 jobs are cycles
      .foreach(scc => errors += s"{${scc.map(job => job.id).mkString(",")}} form a cycle")

    workflow.vertices.groupBy(_.id).collect {
      case (id: String, jobs) if jobs.size > 1 => id
    } foreach (id => errors += s"Id $id is used by more than 1 job")

    errors.toList
  }
}
