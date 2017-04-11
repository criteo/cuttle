package org.criteo.langoustine.examples

object TestExample {

  def main(args: Array[String]): Unit = {
    val example = args.headOption.getOrElse(sys.error("Please specify the example to run as argument"))
    val forked = new ProcessBuilder("java",
                                    "-cp",
                                    System.getProperty("java.class.path"),
                                    s"org.criteo.langoustine.examples.$example").inheritIO.start()

    new Thread() {
      override def run: Unit = {
        println(s"-- example `$example` started, press [Ctrl+D] to quit")
        while (System.in.read != -1) ()
        forked.destroy()
      }
    }.start()

    forked.waitFor
    System.exit(0)
  }

}
