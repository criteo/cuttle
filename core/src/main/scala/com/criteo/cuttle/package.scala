package com.criteo

import scala.concurrent._
import doobie.imports._
import cats.free._

package object cuttle {

  type XA = Transactor[IOLite]
  private[cuttle] val NoUpdate: ConnectionIO[Int] = Free.pure(0)

  type SideEffect[S <: Scheduling] = (Execution[S]) => Future[Unit]

}
