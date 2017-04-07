package org.criteo.langoustine

import java.io.{ File, FileOutputStream, BufferedOutputStream }
import java.time.{ LocalDateTime, ZonedDateTime }
import java.time.format.DateTimeFormatter.{ ISO_INSTANT }

import scala.concurrent.{ Future }
import scala.reflect.{ classTag, ClassTag }
import scala.concurrent.ExecutionContext.Implicits.global

case class ExecutionLog(
  executionId: String,
  context: String,
  contextId: String,
  start: LocalDateTime,
  end: LocalDateTime,
  success: Boolean
)

trait ExecutionStreams {
  def info(str: CharSequence) = this.println("INFO ", str)
  def error(str: CharSequence) = this.println("ERROR", str)
  def debug(str: CharSequence) = this.println("DEBUG", str)
  private def println(tag: String, str: CharSequence): Unit = {
    val time = ZonedDateTime.now().format(ISO_INSTANT)
    str.toString.split("\n").foreach(l => this.println(s"$time $tag - $l"))
  }
  def println(str: CharSequence): Unit
}

case class Execution[S <: Scheduling](
  id: String,
  context: S#Context,
  streams: ExecutionStreams,
  platforms: Seq[ExecutionPlatform[S]]
)

trait ExecutionPlatform[S <: Scheduling]

object ExecutionPlatform {
  implicit def fromExecution[S <: Scheduling](implicit e: Execution[S]): Seq[ExecutionPlatform[S]] = e.platforms
  def lookup[E: ClassTag](implicit platforms: Seq[ExecutionPlatform[_]]): Option[E] = {
    platforms.find(classTag[E].runtimeClass.isInstance).map(_.asInstanceOf[E])
  }
}

case class Executor[S <: Scheduling](platforms: Seq[ExecutionPlatform[S]]) {
  def run(job: Job[S], context: S#Context): Future[Unit] = {
    val nextExecutionId = utils.randomUUID
    val logFile = File.createTempFile("langoustine", nextExecutionId)
    val os = new BufferedOutputStream(new FileOutputStream(logFile))
    val execution = Execution(
      id = nextExecutionId,
      context = context,
      streams = new ExecutionStreams {
        def println(str: CharSequence) = os.write(s"$str\n".getBytes("utf-8"))
      },
      platforms = platforms
    )
    job.effect(execution).andThen { case _ =>
      os.close()
      logFile.delete()
    }
  }
}