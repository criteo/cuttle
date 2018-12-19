package com.criteo.cuttle

import java.time._
import java.util.concurrent.{ExecutorService, TimeUnit}

import scala.util._

import doobie._
import doobie.implicits._
import doobie.hikari._
import io.circe._
import io.circe.syntax._
import io.circe.parser._
import cats.data.NonEmptyList
import cats.implicits._
import cats.effect.{IO, Resource, Sync}
import doobie.util.log

import com.criteo.cuttle.ExecutionStatus._
import com.criteo.cuttle.events.{Event, JobSuccessForced}

/** Configuration of JDBC endpoint.
  *
  * @param host JDBC driver host
  * @param port JDBC driver port
  */
case class DBLocation(host: String, port: Int)

/** Configuration for the MySQL database used by Cuttle.
  *
  * @param locations sequence of JDBC endpoints
  * @param database JDBC database
  * @param username JDBC username
  * @param password JDBC password
  */
case class DatabaseConfig(locations: Seq[DBLocation], database: String, username: String, password: String)

/** Utilities for [[DatabaseConfig]]. */
object DatabaseConfig {

  /** Creates a [[DatabaseConfig]] instance from the following environment variables:
    *
    *  - MYSQL_LOCATIONS (default to `localhost:3306`)
    *  - MYSQL_DATABASE
    *  - MYSQL_USERNAME
    *  - MYSQL_PASSWORD
    */
  def fromEnv: DatabaseConfig = {
    def env(variable: String, default: Option[String] = None) =
      Option(System.getenv(variable)).orElse(default).getOrElse(sys.error(s"Missing env ${'$' + variable}"))

    val dbLocations = env("MYSQL_LOCATIONS", Some("localhost:3306"))
      .split(',')
      .flatMap(_.split(":") match {
        case Array(host, port) => Try(DBLocation(host, port.toInt)).toOption
        case _                 => None
      })

    DatabaseConfig(
      if (dbLocations.nonEmpty) dbLocations else Seq(DBLocation("localhost", 3306)),
      env("MYSQL_DATABASE"),
      env("MYSQL_USERNAME"),
      env("MYSQL_PASSWORD")
    )
  }
}

private[cuttle] object Database {
  implicit val logHandler: log.LogHandler = DoobieLogsHandler(logger).handler

  implicit val ExecutionStatusMeta: Meta[ExecutionStatus] =
    Meta[Boolean].imap(x => if (x) ExecutionSuccessful else ExecutionFailed: ExecutionStatus) {
      case ExecutionSuccessful => true
      case ExecutionFailed     => false
      case x                   => sys.error(s"Unexpected ExecutionLog status to write in database: $x")
    }

  implicit val JsonMeta: Meta[Json] = Meta[String].imap(x => parse(x).fold(e => throw e, identity))(
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
    """.update.run,
    sql"""
      ALTER TABLE paused_jobs ADD COLUMN user VARCHAR(256) NOT NULL DEFAULT 'not defined user', ADD COLUMN date DATETIME NOT NULL DEFAULT '1991-11-01:15:42:00'
    """.update.run,
    sql"""
      ALTER TABLE executions_streams ADD PRIMARY KEY(id)
    """.update.run,
    sql"""
      CREATE TABLE events (
        created      DATETIME NOT NULL,
        user         VARCHAR(256),
        kind         VARCHAR(100),
        job_id       VARCHAR(1000),
        execution_id CHAR(36),
        start_time   DATETIME,
        end_time     DATETIME,
        payload      TEXT NOT NULL
      ) ENGINE = INNODB;

      CREATE INDEX events_by_created ON events (created);
      CREATE INDEX events_by_job_id ON events (job_id);
      CREATE INDEX events_by_execution_id ON events (execution_id);
      CREATE INDEX events_by_start_time ON events (start_time);
      CREATE INDEX events_by_end_time ON events (end_time);
    """.update.run
  )

  private def lockedTransactor(xa: Transactor[IO], releaseIO: IO[Unit]): Transactor[IO] = {
    val guid = java.util.UUID.randomUUID.toString

    // Try to insert our lock at bootstrap
    (for {
      _ <- sql"""CREATE TABLE IF NOT EXISTS locks (
          locked_by       VARCHAR(36) NOT NULL,
          locked_at       DATETIME NOT NULL
        ) ENGINE = INNODB
      """.update.run
      locks <- sql"""
          SELECT locked_by, locked_at FROM locks WHERE TIMESTAMPDIFF(MINUTE, locked_at, NOW()) < 5;
        """.query[(String, Instant)].to[List]
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

    // TODO, verify if it's in daemon mode

    // Refresh our lock every minute (and check that we still are the lock owner)
    ThreadPools
      .newScheduledThreadPool(1, poolName = Some("DatabaseLock"))
      .scheduleAtFixedRate(
        new Runnable {
          def run =
            if ((sql"""
            UPDATE locks SET locked_at = NOW() WHERE locked_by = ${guid}
          """.update.run.transact(xa).unsafeRunSync: Int) != 1) {
              releaseIO.unsafeRunSync()
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

  private[cuttle] val doSchemaUpdates = utils.updateSchema("schema_evolutions", schemaEvolutions)

  private val connections = collection.concurrent.TrieMap.empty[DatabaseConfig, XA]

  private[cuttle] def newHikariTransactor(dbConfig: DatabaseConfig): Resource[IO, HikariTransactor[IO]] = {
    import com.criteo.cuttle.ThreadPools.Implicits.doobieContextShift

    val locationString = dbConfig.locations.map(dbLocation => s"${dbLocation.host}:${dbLocation.port}").mkString(",")
    val jdbcString = s"jdbc:mysql://$locationString/${dbConfig.database}" +
      "?serverTimezone=UTC&useSSL=false&allowMultiQueries=true&failOverReadOnly=false&rewriteBatchedStatements=true"

    for {
      connectThreadPool <- ThreadPools.doobieConnectThreadPoolResource
      transactThreadPool <- ThreadPools.doobieTransactThreadPoolResource
      transactor <- HikariTransactor.newHikariTransactor[IO](
        "com.mysql.cj.jdbc.Driver",
        jdbcString,
        dbConfig.username,
        dbConfig.password,
        connectThreadPool,
        transactThreadPool
      )
    } yield transactor
  }

  // we remove all created Hikari transactors here,
  // it can be handy if you to recreate a connection with same DbConfig
  private[cuttle] def reset(): Unit =
    connections.clear()

  def connect(dbConfig: DatabaseConfig): XA = {
    // FIXME we shouldn't use allocated as it's unsafe instead we have to flatMap on the Resource[HikariTransactor]
    val (transactor, releaseIO) = newHikariTransactor(dbConfig).allocated.unsafeRunSync
    logger.debug("Allocated new Hikari transactor")
    connections.getOrElseUpdate(
      dbConfig, {
        val xa = lockedTransactor(transactor, releaseIO)
        logger.debug("Lock transactor")
        doSchemaUpdates.transact(xa).unsafeRunSync
        logger.debug("Update Cuttle Schema")
        xa
      }
    )
  }
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
      .to[List]
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

  def resumeJob(id: String): ConnectionIO[Int] =
    sql"""
      DELETE FROM paused_jobs WHERE id = $id
    """.update.run

  private[cuttle] def pauseJobQuery[S <: Scheduling](pausedJob: PausedJob) = sql"""
      INSERT INTO paused_jobs VALUES (${pausedJob.id}, ${pausedJob.user}, ${pausedJob.date})
    """.update

  def pauseJob(pausedJob: PausedJob): ConnectionIO[Int] =
    resumeJob(pausedJob.id) *> pauseJobQuery(pausedJob).run

  private[cuttle] val getPausedJobIdsQuery = sql"SELECT id, user, date FROM paused_jobs".query[PausedJob]

  def getPausedJobs: ConnectionIO[Seq[PausedJob]] = getPausedJobIdsQuery.to[Seq]

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
      .to[List]
      .map(_.map {
        case (startTime, endTime, durationSeconds, waitingSeconds, status) =>
          new ExecutionStat(startTime, endTime, durationSeconds, waitingSeconds, status)
      })

  def logEvent(e: Event): ConnectionIO[Int] = {
    val payload = e.asJson
    val query: Update0 = e match {
      case JobSuccessForced(date, user, job, start, end) =>
        sql"""INSERT INTO events (created, user, kind, job_id, start_time, end_time, payload)
              VALUES (${date}, ${user.userId}, ${e.getClass.getSimpleName}, ${job}, ${start}, ${end}, ${payload})""".update
    }
    query.run
  }

  val healthCheck: ConnectionIO[Boolean] =
    sql"""select 1 from dual"""
      .query[Boolean]
      .unique
}

object Queries {
  val getAllContexts: Fragment =
    (sql"""
      SELECT context_id as id, context_id as json FROM executions
    """)
}
