package com.criteo.cuttle

import lol.http.PartialService
import io.circe.Json
import doobie.imports._

import java.util.{Comparator}

trait Scheduler[S <: Scheduling] {
  def start(workflow: Workflow[S], executor: Executor[S], xa: XA): Unit
  private[cuttle] def routes(workflow: Workflow[S], executor: Executor[S], xa: XA): PartialService =
    PartialFunction.empty
  val allContexts: Fragment
}

trait SchedulingContext {
  def toJson: Json
  def log: ConnectionIO[String]
  def compareTo(other: SchedulingContext): Int
}

object SchedulingContext {
  implicit val ordering: Ordering[SchedulingContext] =
    Ordering.comparatorToOrdering(new Comparator[SchedulingContext] {
      def compare(o1: SchedulingContext, o2: SchedulingContext) = o1.compareTo(o2)
    })
}

trait Scheduling {
  type Context <: SchedulingContext
  type DependencyDescriptor
  def toJson: Json
}
