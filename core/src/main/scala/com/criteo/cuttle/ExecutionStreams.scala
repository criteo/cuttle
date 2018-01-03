package com.criteo.cuttle

import java.io._
import java.nio.file.Files
import java.time.Instant

import cats.effect.IO

import doobie.implicits._

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.stm._

/** The scoped output streams for an [[Execution]]. Allows the execution to log its output. */
trait ExecutionStreams {

  /** Output info messages */
  def info(str: CharSequence = "") = this.writeln("INFO ", str)

  /** Output error messages */
  def error(str: CharSequence = "") = this.writeln("ERROR", str)

  /** Output debug messages (usually used by the [[ExecutionPlatform]]) */
  def debug(str: CharSequence = "") = this.writeln("DEBUG", str)
  private def writeln(tag: String, str: CharSequence): Unit = {
    val time = Instant.now.toString
    str.toString.split("\n").foreach(l => this.writeln(s"$time $tag - $l"))
  }
  private[cuttle] def writeln(str: CharSequence): Unit
}

private[cuttle] object ExecutionStreams {
  private type ExecutionId = String
  private type LastUsageTime = Long
  private val transientStorage = Files.createTempDirectory("cuttle-logs").toFile
  private val openHandles = TMap.empty[ExecutionId, (PrintWriter, LastUsageTime)]
  private val maxHandles = 1024
  private val SC = utils.createScheduler("com.criteo.cuttle.ExecutionStreams.SC")

  logger.info(s"Transient execution streams go to $transientStorage")

  private def logFile(id: ExecutionId): File = new File(transientStorage, id)

  private def getWriter(id: ExecutionId): PrintWriter = {
    val now = System.currentTimeMillis
    val maybeWriter = atomic { implicit tx =>
      val h = openHandles.get(id)
      h.foreach { case (w, _) => openHandles += (id -> (w -> now)) }
      h.map(_._1)
    }
    maybeWriter.getOrElse {
      val (w, toClose) = atomic { implicit tx =>
        val w =
          new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logFile(id), true), "utf8")))
        val toClose = if (openHandles.size > maxHandles) {
          val toClear = openHandles.toSeq.sortBy(_._2._2).take(openHandles.size - maxHandles + 1).map(_._1)
          toClear.map { id =>
            val writerToClose = openHandles(id)._1
            openHandles -= id
            writerToClose
          }
        } else Nil
        openHandles += (id -> (w -> now))
        (w, toClose)
      }
      toClose.foreach(_.close())
      w
    }
  }

  def getStreams(id: ExecutionId, queries: Queries, xa: XA): fs2.Stream[IO, Byte] = {
    def go(alreadySent: Int = 0): fs2.Stream[IO, Byte] =
      fs2.Stream.eval(IO { streamsAsString(id) }).flatMap {
        case Some(content) =>
          fs2.Stream.chunk(fs2.Chunk.bytes(content.drop(alreadySent).getBytes("utf8"))) ++ SC.delay(go(content.length),
                                                                                                    1 second)
        case None =>
          fs2.Stream
            .eval(
              queries
                .archivedStreams(id)
                .transact(xa)
                .map(_.map(content => fs2.Stream.chunk(fs2.Chunk.bytes(content.drop(alreadySent).getBytes("utf8"))))
                  .getOrElse(fs2.Stream.raiseError(new Exception(s"Streams not found for execution $id"))))
            )
            .flatMap(x => x)
      }

    go()
  }

  def writeln(id: ExecutionId, msg: CharSequence): Unit = {
    val w = getWriter(id)
    w.println(msg)
    w.flush()
  }

  def streamsAsString(id: ExecutionId): Option[String] = {
    val f = logFile(id)
    if (f.exists) {
      val limit = 1024 * 64
      val buffer = Array.ofDim[Byte](limit)
      val in = new FileInputStream(f)
      try {
        val size = in.read(buffer)
        if (size >= 0) {
          Some {
            val content = new String(buffer, 0, size, "utf8")
            if (f.length > limit) {
              content + "\n--- CONTENT TRUNCATED AT 64kb --"
            } else {
              content
            }
          }
        } else {
          Some("")
        }
      } finally {
        in.close()
      }
    } else {
      None
    }
  }

  def discard(id: ExecutionId): Unit = {
    val toClose = atomic { implicit tx =>
      val w = openHandles.get(id).map(_._1)
      openHandles -= id
      w
    }
    toClose.foreach(_.close())
    logFile(id).delete()
  }

  def archive(id: ExecutionId, queries: Queries, xa: XA): Unit = {
    queries
      .archiveStreams(id, streamsAsString(id).getOrElse(sys.error(s"Cannot archive streams for execution $id")))
      .transact(xa)
      .unsafeRunSync()
    discard(id)
  }
}
