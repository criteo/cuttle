package org.criteo.langoustine

trait Scheduler[S <: Scheduling] {
  def run(graph: Graph[S], executor: Executor): Unit
}

trait SchedulingContext extends Ordered[SchedulingContext]

trait Scheduling {
  type Context <: SchedulingContext
  type DependencyDescriptor
}