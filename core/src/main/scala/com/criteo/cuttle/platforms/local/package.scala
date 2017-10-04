package com.criteo.cuttle.platforms

/** Allow to fork process locally in a managed way.
  *
  * It provides the `sh` interpolator that you can use to fork shell scripts:
  *
  * {{{
  *   sh"""hdfs dfs -cp /from /to""".exec()
  * }}}
  */
package object local {

  /** The __sh__ string interpolation. */
  implicit class InlineCommands(val sc: StringContext) extends AnyVal {
    def sh(args: Any*) =
      LocalPlatform.fork(sc.parts.zipAll(args, "", "").map { case (a, b) => a + b }.mkString.stripMargin)
  }

}
