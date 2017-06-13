package com.criteo

import scala.concurrent._
import doobie.imports._
import cats.free._

package object cuttle {

  type XA = Transactor[IOLite]
  private[cuttle] val NoUpdate: ConnectionIO[Int] = Free.pure(0)

  type SideEffect[S <: Scheduling] = (Execution[S]) => Future[Unit]

  implicit class InlineCommands(val sc: StringContext) extends AnyVal {
    def sh(args: Any*) =
      LocalPlatform.fork(sc.parts.zipAll(args, "", "").map { case (a, b) => a + b }.mkString)
  }

}
