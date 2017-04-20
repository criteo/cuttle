package org.criteo.langoustine

import lol.http.PartialService
import io.circe.Json

trait Scheduler[S <: Scheduling] {
  def run(graph: Graph[S], executor: Executor[S], xa: XA): Unit
  def routes: PartialService = PartialFunction.empty
}

trait SchedulingContext {
  def toJson: Json
}

trait Scheduling {
  type Context <: SchedulingContext
  type DependencyDescriptor
}
