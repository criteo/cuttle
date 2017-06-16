package com.criteo.cuttle.platforms

package object local {

  implicit class InlineCommands(val sc: StringContext) extends AnyVal {
    def sh(args: Any*) =
      LocalPlatform.fork(sc.parts.zipAll(args, "", "").map { case (a, b) => a + b }.mkString)
  }

}
