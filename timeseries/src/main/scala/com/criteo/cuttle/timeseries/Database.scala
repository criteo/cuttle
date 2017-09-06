package com.criteo.cuttle.timeseries

import com.criteo.cuttle._
import com.criteo.cuttle.logging._

import Internal._

import java.time._

import cats.Applicative
import cats.data.{NonEmptyList, OptionT}
import cats.implicits._

import io.circe._
import io.circe.syntax._

import doobie.imports._

private[timeseries] case class Database(logger : Logger) {
  import TimeSeriesUtils._
  import com.criteo.cuttle.Database._
  import com.criteo.cuttle.NoUpdate

  import intervals.{Interval, IntervalMap}

  private implicit val logHandler = createLogHandler(logger)

  val schema = List(
    sql"""
      CREATE TABLE timeseries_state (
        state       JSON NOT NULL,
        date        DATETIME NOT NULL
      ) ENGINE = INNODB;

      CREATE INDEX timeseries_state_by_date ON timeseries_state (date);

      CREATE TABLE timeseries_contexts (
        id          VARCHAR(1000) NOT NULL,
        json        JSON NOT NULL,
        ctx_range   LINESTRING NOT NULL,
        backfill_id CHAR(36) NULL,
        PRIMARY KEY (id)
      ) ENGINE = INNODB;

      CREATE SPATIAL INDEX timeseries_contexts_by_range ON timeseries_contexts (ctx_range);

      CREATE TABLE timeseries_backfills (
        id          CHAR(36) NOT NULL,
        name        VARCHAR(200) NOT NULL,
        description TEXT,
        jobs        TEXT NOT NULL,
        priority    SMALLINT NOT NULL,
        start       DATETIME NOT NULL,
        end         DATETIME NOT NULL,
        created_at  DATETIME NOT NULL,
        created_by VARCHAR(100) NOT NULL,
        status      VARCHAR(100) NOT NULL,
        PRIMARY KEY (id)
      ) ENGINE = INNODB;

      CREATE INDEX timeseries_backfills_by_date ON timeseries_backfills (created_at);
      CREATE INDEX timeseries_backfills_by_status ON timeseries_backfills (status);
    """.update
  )

  val doSchemaUpdates: ConnectionIO[Unit] = {
    for {
      _ <- sql"""
        CREATE TABLE IF NOT EXISTS timeseries (
          schema_version  SMALLINT NOT NULL,
          schema_update   DATETIME NOT NULL
        ) ENGINE = INNODB;
      """.update.run

      currentSchemaVersion <- sql"""
        SELECT MAX(schema_version) FROM timeseries
      """.query[Option[Int]].unique.map(_.getOrElse(0))

      _ <- schema.drop(currentSchemaVersion).zipWithIndex.foldLeft(NoUpdate) {
        case (evolutions, (evolution, i)) =>
          evolutions *> evolution.run *> sql"""
            INSERT INTO timeseries (schema_version, schema_update)
            VALUES (${i + 1}, ${Instant.now()})
          """.update.run
      }
    } yield ()
  }

  def sqlGetContextsBetween(start: Option[Instant], end: Option[Instant]): Fragment =
    sql"""
      SELECT id, json FROM timeseries_contexts
      WHERE MBRIntersects(
        ctx_range,
        LineString(
          Point(-1, ${start.map(_.getEpochSecond).getOrElse(0L)}),
          Point( 1, ${end.map(_.getEpochSecond).getOrElse(Long.MaxValue) - 1L})
        )
      )
    """

  def deserializeState(implicit jobs: Set[Job[TimeSeries]]): ConnectionIO[Option[State]] = {
    type StoredState = List[(String, List[(Interval[Instant], JobState)])]
    OptionT {
      sql"SELECT state FROM timeseries_state ORDER BY date DESC LIMIT 1"
        .query[Json]
        .option
    }.map(_.as[StoredState].right.get.flatMap {
        case (jobId, st) =>
          jobs.find(_.id == jobId).map(job => job -> IntervalMap(st: _*))
      }.toMap)
      .value
  }

  def queryBackfills(where: Option[Fragment] = None) = {
    val select =
      sql"""SELECT id, name, description, jobs, priority, start, end, created_at, status, created_by
            FROM timeseries_backfills"""
    where
      .map(select ++ sql" WHERE " ++ _)
      .getOrElse(select)
      .query[(String, String, String, String, Int, Instant, Instant, Instant, String, String)]
  }

  def getBackfillById(id : String) : ConnectionIO[Option[Json]] = {
    val select =
      sql"""SELECT id, name, description, jobs, priority, start, end, created_at, status, created_by
            FROM timeseries_backfills WHERE id=$id"""
    select.query[(String, String, String, String, Int, Instant, Instant, Instant, String, String)]
      .option
      .map(_.map {
        case (id, name, description, jobs, priority, start, end, created_at, status, created_by) =>
          Json.obj(
            "id" -> id.asJson,
            "name" -> name.asJson,
            "description" -> description.asJson,
            "jobs" -> jobs.asJson,
            "priority" -> priority.asJson,
            "start" -> start.asJson,
            "end" -> end.asJson,
            "created_at" -> created_at.asJson,
            "status" -> status.asJson,
            "created_by" -> created_by.asJson
          )
      })
  }

  def getExecutionLogsForBackfill(id : String) : ConnectionIO[Seq[ExecutionLog]] =
      sql"""
        SELECT e.id, job, start_time, end_time, c.json AS context, success, e.waiting_seconds
          FROM executions e
        JOIN timeseries_contexts c
          ON  c.id = e.context_id
        WHERE c.backfill_id=$id
        ORDER BY c.id DESC
        """.query[(String, String, Instant, Instant, Json, ExecutionStatus, Int)]
        .list
        .map(_.map {
          case (id, job, startTime, endTime, context, status, waitingSeconds) =>
            ExecutionLog(id, job, Some(startTime), Some(endTime), context, status, waitingSeconds = waitingSeconds)
        })

  def createBackfill(backfill: Backfill) =
    sql"""INSERT INTO timeseries_backfills (id, name, description, jobs, priority, start, end, created_at, status, created_by)
          VALUES (${backfill.id},
                  ${backfill.name},
                  ${backfill.description},
                  ${backfill.jobs.map(_.id).mkString(",")},
                  ${backfill.priority},
                  ${backfill.start},
                  ${backfill.end},
                  ${Instant.now()},
                  ${backfill.status},
                  ${backfill.createdBy}
                 )""".update.run

  def setBackfillStatus(ids: Set[String], status: String) =
    (
      sql"UPDATE timeseries_backfills SET status = $status WHERE " ++
        Fragments.in(fr"id", NonEmptyList.fromListUnsafe(ids.toList))
    ).update.run

     def serializeState(state: State): ConnectionIO[Int] = {
    import JobState.{Done, Todo}

    val now = Instant.now()
    val stateJson = state.toList.map {
      case (job, im) =>
        (job.id, im.toList.filter {
          case (interval, jobState) =>
            jobState match {
              case Done => true
              case Todo(_) => true
              case _ => false
            }
        })
    }.asJson
    sql"INSERT INTO timeseries_state (state, date) VALUES (${stateJson}, ${now})".update.run
  }
}

object Database {
  import com.criteo.cuttle.Database._

  def serializeContext(context: TimeSeriesContext): ConnectionIO[String] = {
    sql"""
      REPLACE INTO timeseries_contexts (id, json, ctx_range, backfill_id)
      VALUES (
        ${context.toString},
        ${context.asJson},
        LineString(
          Point(-1, ${context.start.getEpochSecond}),
          Point( 1, ${context.end.getEpochSecond - 1L})
        ),
        ${context.backfill.map(_.id)}
      )
    """.update.run *> Applicative[ConnectionIO].pure(context.toString)
  }  
}
