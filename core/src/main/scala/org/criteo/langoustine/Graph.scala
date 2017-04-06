package org.criteo.langoustine

import scala.concurrent.{ Future }

sealed trait Graph[S <: Scheduling] {
  type Dependency = (Job[S], Job[S], S#DependencyDescriptor)

  private[langoustine] def vertices: Set[Job[S]]
  private[langoustine] def edges: Set[Dependency]

  def and(otherGraph: Graph[S]): Graph[S] = {
    val graph = this
    new Graph[S] {
      val vertices = otherGraph.vertices ++ graph.vertices
      val edges = otherGraph.edges ++ graph.edges
    }
  }

  private[langoustine] lazy val roots = vertices.filter (v =>
      edges.forall { case (v1, _, _) => v1 != v })
  private[langoustine] lazy val leaves = vertices.filter (v =>
      edges.forall { case (_, v2, _) => v2 != v })

  def dependsOn(right: Graph[S])
  (implicit depDescriptor: S#DependencyDescriptor): Graph[S] =
    dependsOn((right, depDescriptor))

  def dependsOn(right: (Graph[S], S#DependencyDescriptor)): Graph[S] = {
    val (rightGraph, depDescriptor) = right
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

case class Job[S <: Scheduling](id: String, scheduling: S)(val effect: Execution[S#Context] => Future[Unit]) extends Graph[S] {
  val vertices = Set(this)
  val edges = Set.empty[Dependency]
}