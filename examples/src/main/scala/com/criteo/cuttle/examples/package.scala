package com.criteo.cuttle

package object examples {

  /** Default implicit logger that output everything to __stdout__ */
  implicit val logger = new Logger {
    def logMe(message: => String, level: String) = println(s"${java.time.Instant.now}\t${level}\t${message}")
    override def info(message: => String): Unit = logMe(message, "INFO")
    override def debug(message: => String): Unit = logMe(message, "DEBUG")
    override def warn(message: => String): Unit = logMe(message, "WARN")
    override def error(message: => String): Unit = logMe(message, "ERROR")
    override def trace(message: => String): Unit = ()
  }
}
