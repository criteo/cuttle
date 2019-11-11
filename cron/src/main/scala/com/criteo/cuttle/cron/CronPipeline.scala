package com.criteo.cuttle.cron

//A non-cyclic DAG, convention is that child depends on parents
trait CronPipeline[A] {

  val vertices: Set[A]
  val edges: Set[Dependency[A]] = Set.empty

  def children: Set[A] = edges.map { case Dependency(child, _) => child }

  def roots: Set[A] = vertices.filter(!children.contains(_))

  private[cron] def parents: Set[A] = edges.map { case Dependency(_, parent) => parent }

  private[cron] def leaves: Set[A] = vertices.filter(!parents.contains(_))

  private[cron] def parentsMap = edges.groupBy { case Dependency(child, _)   => child }
  private[cron] def childrenMap = edges.groupBy { case Dependency(_, parent) => parent }

  def dependsOn(right: CronPipeline[A]): CronPipeline[A] = {
    val left = this
    val newEdges: Set[Dependency[A]] = for {
      v1 <- left.roots
      v2 <- right.leaves
    } yield Dependency(v1, v2)
    new CronPipeline[A] {
      override val vertices = left.vertices ++ right.vertices
      override val edges = left.edges ++ right.edges ++ newEdges
    }
  }

  def and(other: CronPipeline[A]): CronPipeline[A] = {
    val leftWorkflow = this
    new CronPipeline[A] {
      override val vertices = leftWorkflow.vertices ++ other.vertices
      override val edges = leftWorkflow.edges ++ other.edges
    }
  }
}

//Convention is that child depends on parents
case class Dependency[A](child: A, parent: A)
