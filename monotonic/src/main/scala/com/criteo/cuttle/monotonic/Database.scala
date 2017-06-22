package com.criteo.cuttle.monotonic

import java.time.Instant

import cats.Applicative
import cats.implicits._
import doobie.imports._
import com.criteo.cuttle.Database._

object Database {
  import com.criteo.cuttle.NoUpdate

  val schema = List(
    sql"""
      CREATE TABLE monotonic_state (
        state       JSON NOT NULL,
        date        DATETIME NOT NULL
      ) ENGINE = INNODB;

      CREATE INDEX monotonic_state_by_date ON monotonic_state (date);

      CREATE TABLE monotonic_contexts (
        id          VARCHAR(1000) NOT NULL,
        json        JSON NOT NULL,
        PRIMARY KEY (id)
      ) ENGINE = INNODB;
    """.update
  )

  val doSchemaUpdates: ConnectionIO[Unit] = {
    for {
      _ <- sql"""
        CREATE TABLE IF NOT EXISTS monotonic (
          schema_version  SMALLINT NOT NULL,
          schema_update   DATETIME NOT NULL
        ) ENGINE = INNODB;
      """.update.run

      currentSchemaVersion <- sql"""
        SELECT MAX(schema_version) FROM monotonic
      """.query[Option[Int]].unique.map(_.getOrElse(0))

      _ <- schema.drop(currentSchemaVersion).zipWithIndex.foldLeft(NoUpdate) {
        case (evolutions, (evolution, i)) =>
          evolutions *> evolution.run *> sql"""
            INSERT INTO monotonic (schema_version, schema_update)
            VALUES (${i + 1}, ${Instant.now()})
          """.update.run
      }
    } yield ()
  }

  def sqlGetContexts(): Fragment =
    sql"""
      SELECT id, json FROM monotonic_contexts
    """

  def serializeContext(context: MonotonicContext[_]): ConnectionIO[String] =
    sql"""
      REPLACE INTO monotonic_contexts (id, json)
      VALUES (
        ${context.toString},
        ${context.toJson}
      )
    """.update.run *> Applicative[ConnectionIO].pure(context.toString)

}
