package com.criteo.cuttle.platforms

/** Allow to fork process locally in a managed way.
  *
  * It provides the `exec` interpolator that you can use to fork shell commands:
  *
  * {{{
  *   exec"""hdfs dfs -cp /from /to"""()
  * }}}
  *
  *
  * The command your provide will be forked into another process.
  * Note that if you provide several commands separated by `;' only the first one will be forked
  * and the other ones will be ignored.
  *
  * If you really need to run several commands, you can use one of the followed ways:
  *
  * 1. Chain multiple execs in a for-comprehension. Because of platform mechanism
  * it will require several platform allocations to run completely. And we can guarantee
  * that forked process will be killed.
  *
  * 2. Wrap multiple commands in one `sh -c' _In this case we cannot guarantee that
  * all process spawned inside this fork will be successfully killed when we kill the parent process._
  *
  *
  */
package object local {

  /** The __exec__ string interpolation. */
  implicit class InlineCommands(val sc: StringContext) extends AnyVal {
    def exec(args: Any*) =
      LocalPlatform.fork(sc.parts.zipAll(args, "", "").map { case (a, b) => a + b }.mkString.stripMargin)
  }

}
