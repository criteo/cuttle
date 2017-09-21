package com.criteo

import scala.concurrent._
import doobie.imports._
import cats.free._
import cuttle.logging.Logger

package cuttle {
  sealed trait Completed
  case object Completed extends Completed
}

package object cuttle {

  type XA = Transactor[IOLite]
  private[cuttle] val NoUpdate: ConnectionIO[Int] = Free.pure(0)

  type SideEffect[S <: Scheduling] = (Execution[S]) => Future[Completed]

  implicit def scopedExecutionContext(implicit execution: Execution[_]) = execution.executionContext
 
  implicit val logger = new Logger {
    def logMe(message : => String, level: String) = println(s"${java.time.Instant.now}\t${level}\t${message}")
      override def info(message: => String): Unit = logMe(message, "INFO")
      override def debug(message: => String): Unit = logMe(message, "DEBUG")
      override def warning(message: => String): Unit = ()
      override def error(message : => String): Unit = ()
  }
}
