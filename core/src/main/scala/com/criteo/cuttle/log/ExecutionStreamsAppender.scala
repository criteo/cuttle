package com.criteo.cuttle.log

import com.criteo.cuttle.{ExecutionStreams, Executor}
import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.{AppenderSkeleton, Logger, Priority}

class ExecutionStreamsAppender extends AppenderSkeleton {

  override def append(event: LoggingEvent): Unit = {
    val threadName = event.getThreadName
    Executor.getStreams(threadName) match {
      case Some(streams) => doAppend(event, streams)
      case None =>
    }
  }

  private def doAppend(event: LoggingEvent, streams: ExecutionStreams): Unit = {
    val tag = event.getLevel.toInt match {
      case Priority.DEBUG_INT => Some("DEBUG")
      case Priority.ERROR_INT => Some("ERROR")
      case Priority.FATAL_INT => Some("FATAL")
      case Priority.WARN_INT => Some("WARN")
      case Priority.INFO_INT => Some("INFO")
      case _ => None
    }
    tag.foreach(streams.writeln(_, event.getRenderedMessage))
  }

  override def close(): Unit = {
  }

  override def requiresLayout(): Boolean = false
}

object ExecutionStreamsAppender {

  private lazy val appender = new ExecutionStreamsAppender

  // safe to call multiple time
  def registerAsRootLogger(): Unit = {
    Logger.getRootLogger.addAppender(appender)
  }
}