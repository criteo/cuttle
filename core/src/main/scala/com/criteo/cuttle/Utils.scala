package com.criteo.cuttle

import java.util.UUID
import java.util.concurrent.TimeUnit
import java.lang.management.ManagementFactory
import java.time.Instant

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}
import cats.effect.{IO, Resource, Timer}
import cats.Eq
import cats.implicits._
import doobie._
import doobie.hikari.HikariTransactor
import doobie.implicits._
import io.circe.Json
import org.http4s._
import org.http4s.dsl.io._

/** A set of basic utilities useful to write workflows. */
package object utils {

  /** Get a doobie transactor
    *
    * @param config Database configuration
    */
  def transactor(config: DatabaseConfig): Resource[IO, HikariTransactor[IO]] = Database.newHikariTransactor(config)

  /** Executes unapplied schema evolutions
    *
    * @param table Name of the table that keeps track of applied schema changes
    * @param schemaEvolutions List of schema evolutions (should be append-only)
    */
  def updateSchema(table: String, schemaEvolutions: List[ConnectionIO[_]]) =
    for {
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
    } yield ()

  private[cuttle] implicit val timer: cats.effect.Timer[IO] = {
    val timerThreadPool = ThreadPools.newFixedThreadPool(1, poolName = Some("Timer"))
    IO.timer(ExecutionContext.fromExecutorService(timerThreadPool))
  }

  // FIXME this method only exists because Scala couldn't resolve implicits in the context of Executor.scala
  private[cuttle] def awakeEvery(duration: FiniteDuration): fs2.Stream[IO, FiniteDuration] =
    fs2.Stream.awakeEvery[IO](duration)

  /** Creates a  [[scala.concurrent.Future Future]] that resolve automatically
    * after the given duration.
    */
  object Timeout {
    private val scheduler = ThreadPools.newScheduledThreadPool(1, poolName = Some("Timeout"))

    /**
      * @return timer object for IO.sleep
      */
    implicit val timer: Timer[IO] = IO.timer(ExecutionContext.fromExecutor(scheduler))

    /** Creates a  [[scala.concurrent.Future]] that resolve automatically
      * after the given duration.
      *
      * @param timeout Duration for the timeout.
      */
    def apply(timeout: Duration): Future[Unit] = {
      val p = Promise[Unit]()
      scheduler.schedule(
        new Runnable {
          def run(): Unit = p.success(())
        },
        timeout.toMillis,
        TimeUnit.MILLISECONDS
      )
      p.future
    }

    /**
      * Creates a [[cats.effect.IO]] that resolve automatically
      * after the given duration.
      * @param timeout Duration for the timeout.
      * @return
      */
    def applyF(timeout: Duration): IO[Unit] = IO.async { cb =>
      val runnable = new Runnable {
        def run(): Unit = cb(Right(()))
      }

      scheduler.schedule(
        runnable,
        timeout.length,
        timeout.unit
      )
    }
  }

  private[cuttle] object ExecuteAfter {
    def apply[T](delay: Duration)(block: => Future[T])(implicit executionContext: ExecutionContext) =
      Timeout(delay).flatMap(_ => block)(executionContext)
  }

  private[cuttle] val never = Promise[Nothing]().future

  private[cuttle] def randomUUID(): String = UUID.randomUUID().toString

  private[cuttle] def getJVMUptime = ManagementFactory.getRuntimeMXBean.getUptime / 1000

  private[cuttle] def sse[A](thunk: IO[Option[A]],
                             encode: A => IO[Json])(implicit eqInstance: Eq[A]): IO[Response[IO]] = {
    import scala.concurrent.duration._
    import io.circe._
    import fs2.Stream
    import com.criteo.cuttle.ThreadPools.Implicits.serverContextShift

    val stream = (Stream.emit(()) ++ Stream.fixedRate[IO](1.seconds))
      .evalMap[IO, Option[A]](_ => IO.shift.flatMap(_ => thunk))
      .flatMap({
        case Some(x) => Stream(x)
        case None    => Stream.raiseError[IO](new RuntimeException("Could not get result to stream"))
      })
      .changes
      .evalMap[IO, Json](r => encode(r))
      .map(k => ServerSentEvent(k.noSpaces))

    Ok(stream)
  }
}
