package com.criteo.cuttle.timeseries

import com.criteo.cuttle._
import Internal._

import java.time._

import cats.Applicative
import cats.data.OptionT
import cats.implicits._

import io.circe._
import io.circe.syntax._

import doobie.imports._

import continuum.bound._
import continuum.{Interval, IntervalSet}

object Database {
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
            VALUES (${i + 1}, ${LocalDateTime.now()})
          """.update.run
      }
    } yield ()
  }

  def sqlGetContextsBetween(start: Option[LocalDateTime], end: Option[LocalDateTime]): Fragment =
    sql"""
      SELECT id, json FROM timeseries_contexts
      WHERE MBRIntersects(
        ctx_range,
        LineString(
          Point(-1, ${start.map(_.toEpochSecond(ZoneOffset.UTC)).getOrElse(0L)}),
          Point( 1, ${end.map(_.toEpochSecond(ZoneOffset.UTC)).getOrElse(Long.MaxValue)})
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
          Point(-1, ${context.start.toEpochSecond(ZoneOffset.UTC)}),
          Point( 1, ${context.end.toEpochSecond(ZoneOffset.UTC)})
        )
      )
    """.update.run *> Applicative[ConnectionIO].pure(context.toString)

  def deserialize(implicit jobs: Set[Job[TimeSeriesScheduling]]): ConnectionIO[Option[(State, Set[Backfill])]] = {
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
    }.map(_.as[(State, Set[Backfill])].right.get).value
  }

  def serialize(state: State, backfills: Set[Backfill]) = {
    val now = LocalDateTime.now()
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
    val stateJson = (state, backfills).asJson
    sql"INSERT INTO timeseries_state (state, date) VALUES (${stateJson}, ${now})".update.run
  }
}
