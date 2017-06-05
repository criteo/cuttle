package com.criteo.cuttle

import lol.http.PartialService
import io.circe.Json
import doobie.imports._

trait Scheduler[S <: Scheduling] {
  def run(graph: Graph[S], executor: Executor[S], xa: XA): Unit
  def routes(graph: Graph[S], executor: Executor[S], xa: XA): PartialService = PartialFunction.empty
  val allContexts: Fragment
}

trait SchedulingContext {
  def toJson: Json
  def log: ConnectionIO[String]
}

trait Scheduling {
  type Context <: SchedulingContext
  type DependencyDescriptor
}
