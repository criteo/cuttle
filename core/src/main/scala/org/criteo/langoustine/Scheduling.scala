package org.criteo.langoustine

trait Scheduler[S <: Scheduling] {
  def run(graph: Graph[S], executor: Executor[S]): Unit
}

trait SchedulingContext

trait Scheduling {
  type Context <: SchedulingContext
  type DependencyDescriptor
}