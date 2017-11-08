package com.criteo.cuttle.platforms

/** Allow to fork process locally in a managed way.
  *
  * It provides the `exec` interpolator that you can use to fork shell commands:
  *
  * {{{
  *   exec"""hdfs dfs -cp /from /to"""()
  * }}}
  *
  * Your command(s) will be executed in a forked `sh -c exec $command` process.
  * We can provide a guarantee that forked process will be killed when you want
  * to cancel or pause it only when you put a single command into this interpolation.
  * If you use multiple commands you should provide a way to kill all potentially forked
  * children of your process.
  *
  * Actually there are two ways of chaining several commands that are semantically different:
  *
  * 1. Chain multiple execs in a for-comprehension. Because of platform mechanism
  * it will require several platform allocations to run completely. And we can guarantee
  * that forked process will be killed.
  *
  * Example:
  *
  * {{{
  *   for {
  *          _ <- exec"echo Hello"()
  *          _ <- exec"echo Check my project page at https://github.com/criteo/cuttle"()
  *          completed <- exec"sleep 1"()
  *       }
  * }}}
  *
  * 2. Wrap multiple commands in one ```sh -c```. _In this case we cannot guarantee that
  * all process spawned inside this fork will be successfully killed when we kill the parent process._
  *
  * Example:
  *
  * {{{
  *   exec"""sh -c '
  *       |    echo 1
  *       |    echo 2
  *       |    # we fork a process here that will survive
  *       |    sh -c "sleep 10000"
  *       |'"""()
  * }}}
  *
  */
package object local {

  /** The __sh__ string interpolation. */
  implicit class InlineCommands(val sc: StringContext) extends AnyVal {
    def exec(args: Any*) =
      LocalPlatform.fork(sc.parts.zipAll(args, "", "").map { case (a, b) => a + b }.mkString.stripMargin)
  }

}
