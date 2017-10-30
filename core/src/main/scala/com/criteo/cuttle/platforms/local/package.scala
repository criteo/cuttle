package com.criteo.cuttle.platforms

/** Allow to fork process locally in a managed way.
  *
  * It provides the `sh` interpolator that you can use to fork shell commands:
  *
  * {{{
  *   sh"""hdfs dfs -cp /from /to""".exec()
  * }}}
  *
  * __You command(s) will be executed in a forked `sh -c exec $command` process.
  * We can provide a guarantee that forked process will be killed when you want
  * to cancel or pause it only when you put a single command into this interpolation.
  * If you use multiple commands you should provide a way to kill all potentially forked
  * children of your process.__
  */
package object local {

  /** The __sh__ string interpolation. */
  implicit class InlineCommands(val sc: StringContext) extends AnyVal {
    def sh(args: Any*) =
      LocalPlatform.fork(sc.parts.zipAll(args, "", "").map { case (a, b) => a + b }.mkString.stripMargin)
  }

}
