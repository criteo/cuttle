package org.criteo

import doobie.imports._

import cats.free._

package object langoustine {

  type XA = Transactor[IOLite]

  val NoUpdate: ConnectionIO[Int] = Free.pure(0)

  implicit class InlineCommands(val sc: StringContext) extends AnyVal {
    def sh(args: Any*) =
      LocalPlatform.fork(sc.parts.zipAll(args, "", "").map { case (a, b) => a + b }.mkString)
  }

}
