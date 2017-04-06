package org.criteo

package object langoustine {

  implicit class InlineCommands(val sc: StringContext) extends AnyVal {
    def sh(args: Any*) = {
      LocalFramework.fork(sc.parts.zipAll(args, "", "").map { case (a,b) => a + b }.mkString)
    }
  }

}