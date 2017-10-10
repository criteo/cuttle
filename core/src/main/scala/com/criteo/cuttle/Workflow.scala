package com.criteo.cuttle

import scala.concurrent.{Future}

/*
 * The representation of the workflow to be scheduled
 * */
trait Workflow[S <: Scheduling] {
  type Dependency = (Job[S], Job[S], S#DependencyDescriptor)

  def vertices: Set[Job[S]]
  def edges: Set[Dependency]

  // Kahn's algorithm
  // We construct the best linear representation for our DAG. At the
  // same time it checks that there is no cycle.
  private[cuttle] lazy val jobsInOrder: List[Job[S]] = {
    val result = collection.mutable.ListBuffer.empty[Job[S]]
    val edges = collection.mutable.Set(this.edges.toSeq: _*)
    val set = collection.mutable.SortedSet(roots.toSeq: _*)(Ordering.by(_.id))
    while (set.nonEmpty) {
      val n = set.head
      set.remove(n)
      result.append(n)
      edges.collect({ case edge @ (_, `n`, _) => edge }).toList.map {
        case edge @ (m, _, _) =>
          edges.remove(edge)
          if (edges.collect({ case (`m`, _, _) => () }).isEmpty) {
            set.add(m)
          }
      }
    }
    require(edges.isEmpty, "Workflow has at least one cycle")
    result.toList
  }

  def and(rightWorkflow: Workflow[S]): Workflow[S] = {
    val leftWorkflow = this
    new Workflow[S] {
      val vertices = leftWorkflow.vertices ++ rightWorkflow.vertices
      val edges = leftWorkflow.edges ++ rightWorkflow.edges
    }
  }

  private[cuttle] lazy val roots = vertices.filter(v => edges.forall { case (v1, _, _)  => v1 != v })
  private[cuttle] lazy val leaves = vertices.filter(v => edges.forall { case (_, v2, _) => v2 != v })

  def dependsOn(rightWorkflow: Workflow[S])(implicit dependencyDescriptor: S#DependencyDescriptor): Workflow[S] =
    dependsOn((rightWorkflow, dependencyDescriptor))

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

object Workflow {
  def empty[S <: Scheduling]: Workflow[S] = new Workflow[S] {
    def vertices = Set.empty
    def edges = Set.empty
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
  val vertices = Set(this)
  val edges = Set.empty[Dependency]

  def run(execution: Execution[S]): Future[Completed] = effect(execution)
}
