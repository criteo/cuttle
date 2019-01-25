package com.criteo.cuttle.cron
import com.criteo.cuttle.Logger

object Utils {
  implicit val logger: Logger = new Logger {
    override def debug(message: => String): Unit = ()
    override def info(message: => String): Unit = ()
    override def warn(message: => String): Unit = ()
    override def error(message: => String): Unit = ()
    override def trace(message: => String): Unit = ()
  }
}
