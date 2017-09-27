package com.criteo.cuttle

import lol.http.PartialService
import io.circe.Json
import doobie.imports._
import java.util.Comparator

import authentication._

trait Scheduler[S <: Scheduling] extends MetricProvider {
  def start(workflow: Workflow[S], executor: Executor[S], xa: XA, logger: Logger): Unit
  private[cuttle] def publicRoutes(workflow: Workflow[S], executor: Executor[S], xa: XA): PartialService =
    PartialFunction.empty
  private[cuttle] def privateRoutes(workflow: Workflow[S], executor: Executor[S], xa: XA): AuthenticatedService =
    PartialFunction.empty
  val allContexts: Fragment

  def getStats(jobs: Set[String]): Json
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
