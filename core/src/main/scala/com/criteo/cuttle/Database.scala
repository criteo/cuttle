package com.criteo.cuttle

import doobie._
import doobie.implicits._
import doobie.hikari._
import doobie.hikari.implicits._

import io.circe._
import io.circe.parser._

import cats.data.NonEmptyList
import cats.implicits._
import cats.effect.IO

import scala.util._

import java.time._
import java.util.concurrent.{Executors, TimeUnit}

import ExecutionStatus._

/** Configuration for the MySQL database used by cuttle.
  *
  * @param host JDBC driver host
  * @param port JDBC driver port
  * @param database JDBC database
  * @param username JDBC username
  * @param password JDBC password
  */
case class DatabaseConfig(host: String, port: Int, database: String, username: String, password: String)

/** Utilities for [[DatabaseConfig]]. */
object DatabaseConfig {

  /** Creates a [[DatabaseConfig]] instance from the following environment variables:
    *
    *  - MYSQL_HOST (default to `localhost')
    *  - MYSQL_PORT (default to `3306')
    *  - MYSQL_DATABASE
    *  - MYSQL_USERNAME
    *  - MYSQL_PASSWORD
    */
  def fromEnv: DatabaseConfig = {
    def env(variable: String, default: Option[String] = None) =
      Option(System.getenv(variable)).orElse(default).getOrElse(sys.error(s"Missing env ${'$' + variable}"))
    DatabaseConfig(
      env("MYSQL_HOST", Some("localhost")),
      Try(env("MYSQL_PORT", Some("3306")).toInt).getOrElse(3306),
      env("MYSQL_DATABASE"),
      env("MYSQL_USERNAME"),
      env("MYSQL_PASSWORD")
    )
  }
}

private[cuttle] object Database {

  implicit val DateTimeMeta: Meta[Instant] = Meta[java.sql.Timestamp].nxmap(
    x => Instant.ofEpochMilli(x.getTime),
    x => new java.sql.Timestamp(x.toEpochMilli)
  )

  implicit val ExecutionStatusMeta: Meta[ExecutionStatus] = Meta[Boolean].xmap(
    x => if (x) ExecutionSuccessful else ExecutionFailed,
    x =>
      x match {
        case ExecutionSuccessful => true
        case ExecutionFailed     => false
        case x                   => sys.error(s"Unexpected ExecutionLog status to write in database: $x")
    }
  )

  implicit val JsonMeta: Meta[Json] = Meta[String].nxmap(
    x => parse(x).fold(e => throw e, identity),
    x => x.noSpaces
  )

  val schemaEvolutions = List(
    sql"""
      CREATE TABLE executions (
        id          CHAR(36) NOT NULL,
        job         VARCHAR(1000) NOT NULL,
        start_time  DATETIME NOT NULL,
        end_time    DATETIME NOT NULL,
        context_id  VARCHAR(1000) NOT NULL,
        success     BOOLEAN NOT NULL,
        waiting_seconds INT NOT NULL,
        PRIMARY KEY (id)
      ) ENGINE = INNODB;

      CREATE INDEX execution_by_context_id ON executions (context_id);
      CREATE INDEX execution_by_job ON executions (job);
      CREATE INDEX execution_by_start_time ON executions (start_time);

      CREATE TABLE paused_jobs (
        id          VARCHAR(1000) NOT NULL,
        PRIMARY KEY (id)
      ) ENGINE = INNODB;

      CREATE TABLE executions_streams (
        id          CHAR(36) NOT NULL,
        streams     MEDIUMTEXT
      ) ENGINE = INNODB;
    """
  )

  private def lockedTransactor(xa: HikariTransactor[IO]): HikariTransactor[IO] = {
    val guid = java.util.UUID.randomUUID.toString

    // Try to insert our lock at bootstrap
    (for {
      locks <- sql"""
          SELECT locked_by, locked_at FROM locks WHERE TIMESTAMPDIFF(MINUTE, locked_at, NOW()) < 5;
        """.query[(String, Instant)].list
      _ <- if (locks.isEmpty) {
        sql"""
            DELETE FROM locks;
            INSERT INTO locks VALUES (${guid}, NOW());
          """.update.run
      } else {
        sys.error(s"Database already locked: ${locks.head}")
      }
    } yield ()).transact(xa).unsafeRunSync

    // Remove lock on shutdown
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run =
        sql"""
          DELETE FROM locks WHERE locked_by = $guid;
        """.update.run.transact(xa).unsafeRunSync
    })

    // Refresh our lock every minute (and check that we still are the lock owner)
    Executors
      .newScheduledThreadPool(1)
      .scheduleAtFixedRate(
        new Runnable {
          def run =
            if ((sql"""
            UPDATE locks SET locked_at = NOW() WHERE locked_by = ${guid}
          """.update.run.transact(xa).unsafeRunSync: Int) != 1) {
              xa.shutdown.unsafeRunSync
              sys.error(s"Lock has been lost, shutting down the database connection.")
            }
        },
        1,
        1,
        TimeUnit.MINUTES
      )

    // We can now use the transactor safely
    xa
  }

  private val doSchemaUpdates: ConnectionIO[Unit] =
    (for {
      _ <- sql"""
        CREATE TABLE IF NOT EXISTS schema_evolutions (
          schema_version  SMALLINT NOT NULL,
          schema_update   DATETIME NOT NULL,
          PRIMARY KEY     (schema_version)
        ) ENGINE = INNODB;

        CREATE TABLE IF NOT EXISTS locks (
          locked_by       VARCHAR(36) NOT NULL,
          locked_at       DATETIME NOT NULL
        ) ENGINE = INNODB;
      """.update.run

      currentSchemaVersion <- sql"""
        SELECT MAX(schema_version) FROM schema_evolutions
      """.query[Option[Int]].unique.map(_.getOrElse(0))

      _ <- schemaEvolutions.map(_.update).zipWithIndex.drop(currentSchemaVersion).foldLeft(NoUpdate) {
        case (evolutions, (evolution, i)) =>
          evolutions *> evolution.run *> sql"""
            INSERT INTO schema_evolutions (schema_version, schema_update)
            VALUES (${i + 1}, ${Instant.now()})
          """.update.run
      }
    } yield ())

  private val connections = collection.concurrent.TrieMap.empty[DatabaseConfig, XA]
  def connect(c: DatabaseConfig): XA =
    connections.getOrElseUpdate(
      c, {
        val xa = (for {
          hikari <- HikariTransactor[IO](
            "com.mysql.cj.jdbc.Driver",
            s"jdbc:mysql://${c.host}:${c.port}/${c.database}?serverTimezone=UTC&useSSL=false&allowMultiQueries=true",
            c.username,
            c.password
          )
          _ <- hikari.configure { datasource =>
            IO.pure( /* Configure datasource if needed */ ())
          }
        } yield hikari).unsafeRunSync
        doSchemaUpdates.transact(xa).unsafeRunSync
        lockedTransactor(xa)
      }
    )
}

private[cuttle] trait Queries {
  import Database._

  def logExecution(e: ExecutionLog, logContext: ConnectionIO[String]): ConnectionIO[Int] =
    for {
      contextId <- logContext
      result <- sql"""
        INSERT INTO executions (id, job, start_time, end_time, success, context_id, waiting_seconds)
        VALUES (${e.id}, ${e.job}, ${e.startTime}, ${e.endTime}, ${e.status}, ${contextId}, ${e.waitingSeconds})
        """.update.run
    } yield result

  def getExecutionLogSize(jobs: Set[String]): ConnectionIO[Int] =
    (sql"""
      SELECT COUNT(*) FROM executions WHERE """ ++ Fragments.in(fr"job", NonEmptyList.fromListUnsafe(jobs.toList)))
      .query[Int]
      .unique

  def getExecutionLog(contextQuery: Fragment,
                      jobs: Set[String],
                      sort: String,
                      asc: Boolean,
                      offset: Int,
                      limit: Int): ConnectionIO[List[ExecutionLog]] = {
    val orderBy = (sort, asc) match {
      case ("context", true)    => sql"ORDER BY context_id ASC, job, id"
      case ("context", false)   => sql"ORDER BY context_id DESC, job, id"
      case ("job", true)        => sql"ORDER BY job ASC, context_id, id"
      case ("job", false)       => sql"ORDER BY job DESC, context_id, id"
      case ("status", true)     => sql"ORDER BY success ASC, context_id, job, id"
      case ("status", false)    => sql"ORDER BY success DESC, context_id, job, id"
      case ("startTime", true)  => sql"ORDER BY start_time ASC, id"
      case ("startTime", false) => sql"ORDER BY start_time DESC, id"
      case (_, true)            => sql"ORDER BY end_time ASC, id"
      case _                    => sql"ORDER BY end_time DESC, id"
    }
    (sql"""
      SELECT executions.id, job, start_time, end_time, contexts.json AS context, success, executions.waiting_seconds
      FROM executions INNER JOIN (""" ++ contextQuery ++ sql""") contexts
      ON executions.context_id = contexts.id WHERE """ ++ Fragments.in(
      fr"job",
      NonEmptyList.fromListUnsafe(jobs.toList)) ++ orderBy ++ sql""" LIMIT $limit OFFSET $offset""")
      .query[(String, String, Instant, Instant, Json, ExecutionStatus, Int)]
      .list
      .map(_.map {
        case (id, job, startTime, endTime, context, status, waitingSeconds) =>
          ExecutionLog(id, job, Some(startTime), Some(endTime), context, status, waitingSeconds = waitingSeconds)
      })
  }

  def getExecutionById(contextQuery: Fragment, id: String): ConnectionIO[Option[ExecutionLog]] =
    (sql"""
      SELECT executions.id, job, start_time, end_time, contexts.json AS context, success, executions.waiting_seconds
      FROM executions INNER JOIN (""" ++ contextQuery ++ sql""") contexts
      ON executions.context_id = contexts.id WHERE executions.id = $id""")
      .query[(String, String, Instant, Instant, Json, ExecutionStatus, Int)]
      .option
      .map(_.map {
        case (id, job, startTime, endTime, context, status, waitingSeconds) =>
          ExecutionLog(id, job, Some(startTime), Some(endTime), context, status, waitingSeconds = waitingSeconds)
      })

  def unpauseJob[S <: Scheduling](job: Job[S]): ConnectionIO[Int] =
    sql"""
      DELETE FROM paused_jobs WHERE id = ${job.id}
    """.update.run

  def pauseJob[S <: Scheduling](job: Job[S]): ConnectionIO[Int] =
    unpauseJob(job) *> sql"""
      INSERT INTO paused_jobs VALUES (${job.id});
    """.update.run

  def getPausedJobIds: ConnectionIO[Set[String]] =
    sql"SELECT id FROM paused_jobs"
      .query[String]
      .to[Set]

  def archiveStreams(id: String, streams: String): ConnectionIO[Int] =
    sql"""
      INSERT INTO executions_streams (id, streams) VALUES
      (${id}, ${streams})
    """.update.run

  def archivedStreams(id: String): ConnectionIO[Option[String]] =
    sql"SELECT streams FROM executions_streams WHERE id = ${id}"
      .query[String]
      .option

  def jobStatsForLastThirtyDays(jobId: String): ConnectionIO[List[ExecutionStat]] =
    sql"""
         select
             start_time,
             end_time,
             TIMESTAMPDIFF(SECOND, start_time, end_time) as duration_seconds,
             waiting_seconds as waiting_seconds,
             success
         from executions
         where job=$jobId and end_time > DATE_SUB(CURDATE(), INTERVAL 30 DAY) order by start_time asc, end_time asc
       """
      .query[(Instant, Instant, Int, Int, ExecutionStatus)]
      .list
      .map(_.map {
        case (startTime, endTime, durationSeconds, waitingSeconds, status) =>
          new ExecutionStat(startTime, endTime, durationSeconds, waitingSeconds, status)
      })

  val healthCheck: ConnectionIO[Boolean] =
    sql"""select 1 from dual"""
      .query[Boolean]
      .unique
}
