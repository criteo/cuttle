package com.criteo.cuttle

import cats.implicits._

import doobie._
import doobie.implicits._

import io.circe.Json

import scala.concurrent.Future

trait TestScheduling {
  case class TestDependencyDescriptor()

  object TestDependencyDescriptor {
    implicit val defDepDescr: TestDependencyDescriptor = TestDependencyDescriptor()
  }

  case class TestContext() extends SchedulingContext {
    val toJson: Json = Json.Null
    val log: ConnectionIO[String] = "id".pure[ConnectionIO]
    def compareTo(other: SchedulingContext) = 0
    override def longRunningId(): String = toString
  }

  case class TestScheduling(config: Unit = ()) extends Scheduling {
    type Context = TestContext
    type DependencyDescriptor = TestDependencyDescriptor
    type Config = Unit
    def toJson: Json = Json.Null
  }

  val completed: (Execution[_]) => Future[Completed.type] = (_: Execution[_]) => Future.successful(Completed)

  val testScheduling = TestScheduling()
}
