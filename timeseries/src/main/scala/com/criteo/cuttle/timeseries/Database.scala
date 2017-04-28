package com.criteo.cuttle.timeseries

import com.criteo.cuttle._
import Internal._

import java.time._

import cats.implicits._
import cats.data.OptionT

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

      CREATE INDEX state_by_date ON timeseries_state (date);
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

  def deserialize(implicit jobs: Set[Job[TimeSeriesScheduling]]): ConnectionIO[Option[(State, Map[Backfill, State])]] = {
    implicit def intervalSetDecoder[A: Ordering](implicit decoder: Decoder[A]): Decoder[IntervalSet[A]] = {
      implicit val intervalDecoder: Decoder[Interval[A]] =
        Decoder.decodeTuple2[A, A].map(x => Interval.closedOpen(x._1, x._2))
      Decoder.decodeList[Interval[A]].map(IntervalSet(_: _*))
    }
    implicit def jobKeyDecoder[A <: Scheduling](implicit js: Set[Job[A]]): KeyDecoder[Job[A]] =
      KeyDecoder.decodeKeyString.map(id => js.find(_.id == id).get)
    implicit val backfillStateDecoder: Decoder[Map[Backfill, State]] =
      Decoder.decodeList[(Backfill, State)].map(Map(_: _*))
    OptionT {
      sql"SELECT state FROM timeseries_state ORDER BY date DESC LIMIT 1"
        .query[Json]
        .option
    }.map(_.as[(State, Map[Backfill, State])].right.get).value
  }

  def serialize(state: State, backfillState: Map[Backfill, State]) = {
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
    implicit val backfillStateEncoder: Encoder[Map[Backfill, State]] =
      Encoder.encodeList[(Backfill, State)].contramap(_.toList)
    val stateJson = (state, backfillState).asJson
    sql"INSERT INTO timeseries_state (state, date) VALUES (${stateJson}, ${now})".update.run
  }
}
