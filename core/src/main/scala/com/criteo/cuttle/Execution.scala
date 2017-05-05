package com.criteo.cuttle

import java.io.{BufferedOutputStream, File, FileOutputStream}
import java.time.{LocalDateTime, ZonedDateTime}
import java.time.format.DateTimeFormatter.{ISO_INSTANT}

import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.stm._
import scala.concurrent.stm.Txn.ExternalDecider
import scala.reflect.{classTag, ClassTag}

import doobie.imports._

import io.circe._

case class ExecutionLog(
  id: String,
  job: String,
  startTime: LocalDateTime,
  endTime: LocalDateTime,
  context: Json,
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
  def lookup[E: ClassTag](implicit platforms: Seq[ExecutionPlatform[_]]): Option[E] =
    platforms.find(classTag[E].runtimeClass.isInstance).map(_.asInstanceOf[E])
}

case class Executor[S <: Scheduling](platforms: Seq[ExecutionPlatform[S]], queries: Queries, xa: XA) {

  val pausedExecutions = TMap.empty[String, Map[S#Context, Promise[Unit]]]

  val pausedIds = queries.getPausedJobIds.transact(xa).unsafePerformIO
  pausedExecutions.single ++= pausedIds.map((_, Map.empty[S#Context, Promise[Unit]]))

  def unpauseJob(job: Job[S]): Unit = {
    val executions = atomic { implicit txn =>
      Txn.setExternalDecider(new ExternalDecider {
        def shouldCommit(implicit txn: InTxnEnd): Boolean = {
          queries.unpauseJob(job).transact(xa).unsafePerformIO
          true
        }
      })
      val excs = pausedExecutions.get(job.id)
      pausedExecutions -= job.id
      excs
    }
    executions.toList.flatMap(_.toList).foreach {
      case (context, promise) =>
        promise.completeWith(run(job, context))
    }
  }

  def pauseJob(job: Job[S]): Unit =
    atomic { implicit txn =>
      Txn.setExternalDecider(new ExternalDecider {
        def shouldCommit(implicit txn: InTxnEnd): Boolean = {
          queries.pauseJob(job).transact(xa).unsafePerformIO
          true
        }
      })
      pausedExecutions += (job.id -> Map.empty)
    }

  def run(job: Job[S], context: S#Context): Future[Unit] = {
    val maybePausedExecution: Option[Future[Unit]] = atomic { implicit txn =>
      for {
        executions <- pausedExecutions.get(job.id)
      } yield {
        def newPromise() = {
          val promise = Promise[Unit]()
          pausedExecutions.update(job.id, executions + (context -> promise))
          promise
        }
        executions.getOrElse(context, newPromise()).future
      }
    }
    maybePausedExecution.getOrElse {
      val nextExecutionId = utils.randomUUID
      val logFile = File.createTempFile("cuttle", nextExecutionId)
      val os = new BufferedOutputStream(new FileOutputStream(logFile))
      val execution = Execution(
        id = nextExecutionId,
        context = context,
        streams = new ExecutionStreams {
          def println(str: CharSequence) = os.write(s"$str\n".getBytes("utf-8"))
        },
        platforms = platforms
      )
      val startTime = LocalDateTime.now()
      job.effect(execution).andThen {
        case result =>
          os.close()
          logFile.delete()
          queries
            .logExecution(
              ExecutionLog(
                nextExecutionId,
                job.id,
                startTime,
                LocalDateTime.now(),
                context.toJson,
                result.isSuccess
              ))
            .transact(xa)
            .unsafePerformIO
      }
    }
  }

  def getExecutionLog(success: Boolean): List[ExecutionLog] =
    queries.getExecutionLog(success).transact(xa).unsafePerformIO
}
