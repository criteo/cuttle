package com.criteo.cuttle

/** A logger used to output internal informations. */
trait Logger {
  def debug(message: => String): Unit
  def info(message: => String): Unit
  def warn(message: => String): Unit
  def error(message: => String): Unit
  def trace(message: => String): Unit
}