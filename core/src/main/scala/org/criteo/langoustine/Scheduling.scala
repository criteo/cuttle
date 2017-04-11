package org.criteo.langoustine

import lol.http.{PartialService}

trait Scheduler[S <: Scheduling] {
  def run(graph: Graph[S], executor: Executor[S]): Unit
  def routes: PartialService = PartialFunction.empty
}

trait SchedulingContext

trait Scheduling {
  type Context <: SchedulingContext
  type DependencyDescriptor
}
