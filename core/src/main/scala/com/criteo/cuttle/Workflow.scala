package com.criteo.cuttle

import scala.concurrent.{Future}

/*
 * The representation of the workflow to be scheduled
 * */
sealed trait Workflow[S <: Scheduling] {
  type Dependency = (Job[S], Job[S], S#DependencyDescriptor)

  private[cuttle] def vertices: Set[Job[S]]
  private[cuttle] def edges: Set[Dependency]

  def and(rightWorkflow: Workflow[S]): Workflow[S] = {
    val leftWorkflow = this
    new Workflow[S] {
      val vertices = leftWorkflow.vertices ++ rightWorkflow.vertices
      val edges = leftWorkflow.edges ++ rightWorkflow.edges
    }
  }

  private[cuttle] lazy val roots = vertices.filter(v => edges.forall { case (v1, _, _) => v1 != v })
  private[cuttle] lazy val leaves = vertices.filter(v => edges.forall { case (_, v2, _) => v2 != v })

  def dependsOn(rightWorkflow: Workflow[S])(implicit depDescriptor: S#DependencyDescriptor): Workflow[S] =
    dependsOn((rightWorkflow, depDescriptor))

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

/*
 * A tag is used to annotate a job.
 * */
case class Tag(name: String, description: String = "")

case class Job[S <: Scheduling](id: String,
                                scheduling: S,
                                name: String = "",
                                description: String = "",
                                tags: Set[Tag] = Set.empty[Tag])(val effect: SideEffect[S])
    extends Workflow[S] {
  private[cuttle] val vertices = Set(this)
  private[cuttle] val edges = Set.empty[Dependency]

  def run(execution: Execution[S]): Future[Unit] = effect(execution)
}
