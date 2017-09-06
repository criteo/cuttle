package com.criteo.cuttle.logging

trait Logger {
  def debug(message: => String): Unit
  def info(message: => String): Unit
  def warning(message: => String): Unit
  def error(message: => String): Unit
}

object default {
  implicit val logger = new Logger {
    def logMe(message : => String, level: String) = println(s"${java.time.Instant.now}\t${level}\t${message}")
      override def info(message: => String): Unit = logMe(message, "INFO")
      override def debug(message: => String): Unit = logMe(message, "DEBUG")
      override def warning(message: => String): Unit = ()
      override def error(message : => String): Unit = ()
  }
}