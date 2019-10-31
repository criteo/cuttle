package com.criteo.cuttle.timeseries

import com.criteo.cuttle._
import Internal._
import java.time._

import scala.concurrent.duration.Duration
import cats.Applicative
import cats.data.{NonEmptyList, OptionT}
import cats.implicits._
import com.criteo.cuttle.timeseries.JobState.Todo
import io.circe._
import io.circe.syntax._
import doobie._
import doobie.implicits._

private[timeseries] object Database {
  import TimeSeriesUtils._
  import com.criteo.cuttle.Database._

  import intervals.{Interval, IntervalMap}

  val contextIdMigration: ConnectionIO[Unit] = {
    implicit val jobs: Set[TimeSeriesJob] = Set.empty
    val chunkSize = 1024 * 10
    val stream = sql"SELECT id, json FROM timeseries_contexts"
      .query[(String, Json)]
      .streamWithChunkSize(chunkSize)
    val insert = Update[(String, String)]("INSERT into tmp (id, new_id) VALUES (? , ?)")
    for {
      _ <- sql"CREATE TEMPORARY TABLE tmp (id VARCHAR(1000), new_id VARCHAR(1000))".update.run
      _ <- stream
        .chunkLimit(chunkSize)
        .evalMap { oldContexts =>
          insert.updateMany(oldContexts.map {
            case (id, json) =>
              (id, json.as[TimeSeriesContext].right.get.toId)
          })
        }
        .compile
        .drain
      _ <- sql"""CREATE INDEX tmp_id ON tmp (id)""".update.run
      _ <- sql"""UPDATE timeseries_contexts ctx JOIN tmp ON ctx.id = tmp.id
                 SET ctx.id = tmp.new_id""".update.run
      _ <- sql"""UPDATE executions JOIN tmp ON executions.context_id = tmp.id
                 SET executions.context_id = tmp.new_id""".update.run
    } yield ()
  }

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
    """.update.run,
    contextIdMigration,
    NoUpdate, // We removed this migration, so we reserve this slot
    sql"DROP INDEX IF EXISTS `PRIMARY` ON timeseries_state".update.run
  )

  val doSchemaUpdates: ConnectionIO[Unit] = utils.updateSchema("timeseries", schema)

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

  def serializeContext(context: TimeSeriesContext): ConnectionIO[String] = {
    val id = context.toId
    sql"""
      REPLACE INTO timeseries_contexts (id, json, ctx_range, backfill_id)
      VALUES (
        ${id},
        ${context.asJson},
        LineString(
          Point(-1, ${context.start.getEpochSecond}),
          Point( 1, ${context.end.getEpochSecond - 1L})
        ),
        ${context.backfill.map(_.id)}
      )
    """.update.run *> Applicative[ConnectionIO].pure(id)
  }

  def dbStateDecoder(json: Json)(implicit jobs: Set[Job[TimeSeries]], backfills: List[Backfill]): Option[State] = {
    type StoredState = List[(String, List[(Interval[Instant], JobState)])]
    json
      .as[StoredState]
      .right
      .toOption
      .map(_.flatMap {
        case (jobId, st) =>
          jobs.find(_.id == jobId).map(job => job -> IntervalMap(st: _*))
      }.toMap)
  }

  def dbStateEncoder(state: State): Json =
    state.toList.map {
      case (job, im) =>
        (job.id, im.toList.filter {
          case (_, jobState) =>
            jobState match {
              case JobState.Done(_) => true
              case JobState.Todo(_, _) => true
              case _                => false
            }
        })
    }.asJson

  def deserializeState(implicit jobs: Set[Job[TimeSeries]], backfills: List[Backfill]): ConnectionIO[Option[State]] =
    OptionT {
      sql"SELECT state FROM timeseries_state ORDER BY date DESC LIMIT 1"
        .query[Json]
        .option
    }.map(json => dbStateDecoder(json).get).value

  def serializeState(state: State, retention: Option[Duration]): ConnectionIO[Int] = {

    val now = Instant.now()
    val cleanStateBefore = retention.map { duration =>
      if (duration.toSeconds <= 0)
        sys.error(s"State retention is badly configured: ${duration}")
      else
        now.minusSeconds(duration.toSeconds)
    }
    val stateJson = dbStateEncoder(state.mapValues(_.map{
      case Todo(backfill, _) => Todo(backfill, None)
      case other => other
    }))

    for {
      // Apply state retention if needed
      _ <- cleanStateBefore
        .map { t =>
          sql"DELETE FROM timeseries_state where date < ${t}".update.run
        }
        .getOrElse(NoUpdate)
      // Insert the latest state
      x <- sql"INSERT INTO timeseries_state (state, date) VALUES (${stateJson}, ${now})".update.run
    } yield x
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

  def getBackfillById(id: String): ConnectionIO[Option[Json]] = {
    val select =
      sql"""SELECT id, name, description, jobs, priority, start, end, created_at, status, created_by
            FROM timeseries_backfills WHERE id=$id"""
    select
      .query[(String, String, String, String, Int, Instant, Instant, Instant, String, String)]
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

  def getExecutionLogsForBackfill(id: String): ConnectionIO[Seq[ExecutionLog]] =
    sql"""
        SELECT e.id, job, start_time, end_time, c.json AS context, success, e.waiting_seconds
          FROM executions e
        JOIN timeseries_contexts c
          ON  c.id = e.context_id
        WHERE c.backfill_id=$id
        ORDER BY c.id DESC
        """
      .query[(String, String, Instant, Instant, Json, ExecutionStatus, Int)]
      .to[List]
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
}
