package com.criteo.cuttle.logging

trait Logger {
  def debug(message: => String): Unit
  def info(message: => String): Unit
  def warning(message: => String): Unit
  def error(message: => String): Unit
}