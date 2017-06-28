package com.criteo.cuttle.timeseries

import com.criteo.cuttle._
import Internal._

import java.time._

import cats.Applicative
import cats.data.{NonEmptyList, OptionT}
import cats.implicits._

import io.circe._
import io.circe.syntax._

import doobie.imports._

import continuum.bound._
import continuum.{Interval, IntervalSet}

private[timeseries] object Database {
  import TimeSeriesUtils._
  import com.criteo.cuttle.Database._
  import com.criteo.cuttle.NoUpdate

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

  def serializeContext(context: TimeSeriesContext): ConnectionIO[String] =
    sql"""
      REPLACE INTO timeseries_contexts (id, json, ctx_range)
      VALUES (
        ${context.toString},
        ${context.asJson},
        LineString(
          Point(-1, ${context.start.getEpochSecond}),
          Point( 1, ${context.end.getEpochSecond - 1L})
        )
      )
    """.update.run *> Applicative[ConnectionIO].pure(context.toString)

  def deserializeState(implicit jobs: Set[Job[TimeSeries]]): ConnectionIO[Option[State]] = {
    implicit def intervalSetDecoder[A: Ordering](implicit decoder: Decoder[A]): Decoder[IntervalSet[A]] = {
      implicit val intervalDecoder: Decoder[Interval[A]] =
        Decoder.decodeTuple2[A, A].map(x => Interval.closedOpen(x._1, x._2))
      Decoder.decodeList[Interval[A]].map(IntervalSet(_: _*))
    }
    implicit def jobKeyDecoder[A <: Scheduling](implicit js: Set[Job[A]]): KeyDecoder[Job[A]] =
      KeyDecoder.decodeKeyString.map(id => js.find(_.id == id).get)
    OptionT {
      sql"SELECT state FROM timeseries_state ORDER BY date DESC LIMIT 1"
        .query[Json]
        .option
    }.map(_.as[State].right.get).value
  }

  def serializeState(state: State): ConnectionIO[Int] = {
    val now = Instant.now()
    implicit def intervalSetEncoder[A](implicit encoder: Encoder[A]): Encoder[IntervalSet[A]] = {
      implicit val intervalEncoder: Encoder[Interval[A]] =
        Encoder.encodeTuple2[A, A].contramap { interval =>
          val Closed(start) = interval.lower.bound
          val Open(end) = interval.upper.bound
          (start, end)
        }
      Encoder.encodeList[Interval[A]].contramap(_.toList)
    }
    implicit def jobKeyEncoder[A <: Scheduling]: KeyEncoder[Job[A]] =
      KeyEncoder.encodeKeyString.contramap(_.id)
    sql"INSERT INTO timeseries_state (state, date) VALUES (${state.asJson}, $now)".update.run
  }

  def queryBackfills =
    sql"""SELECT id, name, description, jobs, priority, start, end, created_at, status
          FROM timeseries_backfills
       """.query[(String, String, String, String, Int, Instant, Instant, Instant, String)]

  def createBackfill(backfill: Backfill) =
    sql"""INSERT INTO timeseries_backfills (id, name, description, jobs, priority, start, end, created_at, status)
          VALUES (${backfill.id},
                  ${backfill.name},
                  ${backfill.description},
                  ${backfill.jobs.map(_.id).mkString(",")},
                  ${backfill.priority},
                  ${backfill.start},
                  ${backfill.end},
                  ${Instant.now()},
                  ${backfill.status}
                 )""".update.run

  def setBackfillStatus(ids: Set[String], status: String) =
    (
      sql"UPDATE timeseries_backfills SET status = $status WHERE " ++
        Fragments.in(fr"id", NonEmptyList.fromListUnsafe(ids.toList))
    ).update.run
}
