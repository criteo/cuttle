package com.criteo.cuttle

import doobie._
import doobie.implicits._

import java.util.UUID
import java.util.concurrent.{Executors, ThreadFactory, TimeUnit}
import java.lang.management.ManagementFactory
import java.time.Instant

import cats.implicits._
import cats.effect.IO

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, Promise}

import lol.http.{PartialService, Service}

/** A set of basic utilities useful to write workflows. */
package object utils {

  /** Executes unapplied schema evolutions
    *
    * @param table Name of the table that keeps track of applied schema changes
    * @param schemaEvolutions List of schema evolutions (should be append-only)
    */
  def updateSchema(table: String, schemaEvolutions: List[ConnectionIO[_]]) =
    (for {
      _ <- Fragment.const(s"""
        CREATE TABLE IF NOT EXISTS ${table} (
          schema_version  SMALLINT NOT NULL,
          schema_update   DATETIME NOT NULL,
          PRIMARY KEY     (schema_version)
        ) ENGINE = INNODB;
      """).update.run

      currentSchemaVersion <- Fragment.const(s"""
        SELECT MAX(schema_version) FROM ${table}
      """).query[Option[Int]].unique.map(_.getOrElse(0))

      _ <- schemaEvolutions.zipWithIndex.drop(currentSchemaVersion).foldLeft(NoUpdate) {
        case (evolutions, (evolution, i)) =>
          val insertEvolutionQuery =
            fr"INSERT INTO" ++ Fragment.const(table) ++ fr"(schema_version, schema_update)" ++
            fr"VALUES(${i + 1}, ${Instant.now()})"
          evolutions *> evolution *> insertEvolutionQuery.update.run
      }
    } yield ())

  private[cuttle] def createScheduler(threadPrefix: String): fs2.Scheduler =
    fs2.Scheduler.allocate[IO](1, daemon = true, threadPrefix, exitJvmOnFatalError = false).unsafeRunSync._1

  /** Creates a  [[scala.concurrent.Future Future]] that resolve automatically
    * after the given duration.
    */
  object Timeout {
    private val scheduler = Executors.newScheduledThreadPool(1, createDaemonThreadFactory())

    /** Creates a  [[scala.concurrent.Future]] that resolve automatically
      * after the given duration.
      *
      * @param timeout Duration for the timeout.
      */
    def apply(timeout: Duration): Future[Unit] = {
      val p = Promise[Unit]()
      scheduler.schedule(
        new Runnable { def run(): Unit = p.success(()) },
        timeout.toMillis,
        TimeUnit.MILLISECONDS
      )
      p.future
    }
  }

  private[cuttle] object ExecuteAfter {
    def apply[T](delay: Duration)(block: => Future[T])(implicit executionContext: ExecutionContext) =
      Timeout(delay).flatMap(_ => block)(executionContext)
  }

  private[cuttle] val never = Promise[Nothing]().future

  private[cuttle] def randomUUID(): String = UUID.randomUUID().toString

  /**
    * Allows chaining of method orFinally
    * from a PartialService that returns a
    * non-further-chainable Service.
    */
  implicit private[cuttle] class PartialServiceConverter(val service: PartialService) extends AnyVal {
    def orFinally(finalService: Service): Service =
      service.orElse(toPartial(finalService))

    private def toPartial(service: Service): PartialService = {
      case e => service(e)
    }
  }

  private[cuttle] def getJVMUptime = ManagementFactory.getRuntimeMXBean.getUptime / 1000

  private[cuttle] def createDaemonThreadFactory(): ThreadFactory {
    def newThread(r: Runnable): Thread
  } = new ThreadFactory() {
    def newThread(r: Runnable): Thread = {
      val t = Executors.defaultThreadFactory.newThread(r)
      t.setDaemon(true)
      t
    }
  }

  private[cuttle] def createThreadFactory(newThreadImpl: Runnable => Thread): ThreadFactory {
    def newThread(r: Runnable): Thread
  } = new ThreadFactory() {
    def newThread(r: Runnable): Thread = newThreadImpl(r)
  }
}
