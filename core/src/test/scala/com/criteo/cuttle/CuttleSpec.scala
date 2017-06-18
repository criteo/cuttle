package com.criteo.cuttle

import scala.concurrent.{Future}

import org.scalatest.{FunSuite}

import io.circe.Json

import doobie.imports._

import cats.Applicative

case class TestDependencyDescriptor()
object TestDependencyDescriptor {
  implicit val defDepDescr = TestDependencyDescriptor()
}
case class TestContext() extends SchedulingContext {
  val toJson = Json.Null
  val log = Applicative[ConnectionIO].pure("id")
  def compareTo(other: SchedulingContext) = 0
}

case class TestScheduling(config: Unit = ()) extends Scheduling {
  type Context = TestContext
  type DependencyDescriptor = TestDependencyDescriptor
  type Config = Unit
}

class LangoustinePPSpec extends FunSuite {
  test("Graph building") {
    val jobs =
      (0 to 3).map(i => Job(i.toString, TestScheduling())(_ => Future.successful(())))
    val graph = (jobs(1) and jobs(2)) dependsOn jobs(0) dependsOn jobs(3)
    assert(graph.vertices.size == 4)
    assert(graph.edges.size == 3)
  }
}
