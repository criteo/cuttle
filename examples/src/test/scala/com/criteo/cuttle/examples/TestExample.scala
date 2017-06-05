package com.criteo.cuttle.examples

import java.io._

object TestExample {

  def run(example: String): Unit = {
    val exampleJVM =
      new ProcessBuilder("java", "-cp", System.getProperty("java.class.path"), s"com.criteo.cuttle.examples.$example")

    exampleJVM.environment.put("MYSQL_HOST", "localhost")
    exampleJVM.environment.put("MYSQL_PORT", "3388")
    exampleJVM.environment.put("MYSQL_DATABASE", "cuttle_dev")
    exampleJVM.environment.put("MYSQL_USER", "root")
    exampleJVM.environment.put("MYSQL_PASSWORD", "")

    val forked = exampleJVM.inheritIO.start()

    new Thread() {
      override def run: Unit = {
        println(s"-- example `$example` started, press [Ctrl+D] to quit")
        while (System.in.read != -1) ()
        forked.destroy()
      }
    }.start()

    forked.waitFor
  }

  def main(args: Array[String]): Unit = {
    val example = args.headOption.getOrElse(sys.error("Please specify the example to run as argument"))

    if (sys.env.get("NO_MYSQL").exists(_ == "true")) {
      run(example)
      System.exit(0)
    } else {
      println("-- Starting a local database")

      val localdb =
        new ProcessBuilder("java", "-cp", System.getProperty("java.class.path"), "com.criteo.cuttle.localdb.LocalDB")
          .start()

      val in = new BufferedReader(new InputStreamReader(localdb.getInputStream))
      var line = ""

      while ({ line = in.readLine; line != null }) {
        println(line)
        if (line == "started!") {
          run(example)
          localdb.destroy()
          System.exit(0)
        }
      }
    }
  }

}
