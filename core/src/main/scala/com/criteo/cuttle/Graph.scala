package com.criteo.cuttle

import scala.concurrent.{Future}

/*
 * The representation of the workflow to be scheduled
 * */
sealed trait Graph[S <: Scheduling] {
  type Dependency = (Job[S], Job[S], S#DependencyDescriptor)

  private[cuttle] def vertices: Set[Job[S]]
  private[cuttle] def edges: Set[Dependency]

  def and(rightGraph: Graph[S]): Graph[S] = {
    val leftGraph = this
    new Graph[S] {
      val vertices = leftGraph.vertices ++ rightGraph.vertices
      val edges = leftGraph.edges ++ rightGraph.edges
    }
  }

  private[cuttle] lazy val roots = vertices.filter(v => edges.forall { case (v1, _, _) => v1 != v })
  private[cuttle] lazy val leaves = vertices.filter(v => edges.forall { case (_, v2, _) => v2 != v })

  def dependsOn(rightGraph: Graph[S])(implicit depDescriptor: S#DependencyDescriptor): Graph[S] =
    dependsOn((rightGraph, depDescriptor))

  def dependsOn(rightOperand: (Graph[S], S#DependencyDescriptor)): Graph[S] = {
    val (rightGraph, depDescriptor) = rightOperand
    val leftGraph = this
    val newEdges: Set[Dependency] = for {
      v1 <- leftGraph.roots
      v2 <- rightGraph.leaves
    } yield (v1, v2, depDescriptor)
    new Graph[S] {
      val vertices = leftGraph.vertices ++ rightGraph.vertices
      val edges = leftGraph.edges ++ rightGraph.edges ++ newEdges
    }
  }
}

/*
 * A tag is used to annotate a job.
 * */
case class Tag(name: String, description: Option[String] = None, color: Option[String] = None)

case class Job[S <: Scheduling](id: String,
                                name: Option[String] = None,
                                description: Option[String] = None,
                                tags: Set[Tag] = Set.empty[Tag],
                                scheduling: S)(val effect: Execution[S] => Future[Unit])
    extends Graph[S] {
  val vertices = Set(this)
  val edges = Set.empty[Dependency]
}
