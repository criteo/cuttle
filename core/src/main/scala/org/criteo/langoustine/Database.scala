package org.criteo.langoustine

import doobie.imports._
import doobie.hikari.imports._

import cats.implicits._

import io.circe._
import io.circe.parser._

import org.postgresql.util._

import scala.util._

import java.time._

case class DatabaseConfig(host: String, port: Int, database: String, user: String, password: String)

object Database {

  implicit val DateTimeMeta: Meta[LocalDateTime] = Meta[java.sql.Timestamp].nxmap(
    x => LocalDateTime.ofInstant(Instant.ofEpochMilli(x.getTime()), ZoneOffset.of("Z")),
    x => new java.sql.Timestamp(x.toInstant(ZoneOffset.of("Z")).toEpochMilli)
  )

  implicit val JsonMeta: Meta[Json] = {
    Meta
      .other[PGobject]("json")
      .nxmap[Json](
        x => parse(x.getValue).fold(e => throw e, identity),
        x => {
          val o = new PGobject
          o.setType("json")
          o.setValue(x.noSpaces)
          o
        }
      )
  }

  val schemaEvolutions = List(
    sql"""
      CREATE TABLE executions (
        id          VARCHAR NOT NULL UNIQUE,
        job         VARCHAR NOT NULL,
        start_time  TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        end_time    TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        context     JSONB NOT NULL,
        success     BOOLEAN NOT NULL
      )
    """.update
  )

  private val doSchemaUpdates =
    (for {
      _ <- sql"""
        CREATE TABLE IF NOT EXISTS schema_evolutions (
          schema_version SMALLINT NOT NULL,
          schema_update  TIMESTAMP WITHOUT TIME ZONE NOT NULL
        )
      """.update.run

      currentSchemaVersion <- sql"""
        SELECT MAX(schema_version) FROM schema_evolutions
      """.query[Option[Int]].unique.map(_.getOrElse(0))

      _ <- schemaEvolutions.drop(currentSchemaVersion).zipWithIndex.foldLeft(NoUpdate) {
        case (evolutions, (evolution, i)) =>
          evolutions *> evolution.run *> sql"""
            INSERT INTO schema_evolutions (schema_version, schema_update)
            VALUES (${i + 1}, ${LocalDateTime.now()})
          """.update.run
      }
    } yield ())

  def connect(c: DatabaseConfig): XA = {
    val xa = (for {
      hikari <- HikariTransactor[IOLite](
        "org.postgresql.Driver",
        s"jdbc:postgresql://${c.host}:${c.port}/${c.database}",
        c.user,
        c.password
      )
      _ <- hikari.configure { datasource =>
        IOLite.primitive( /* Configure datasource if needed */ ())
      }
    } yield hikari).unsafePerformIO
    doSchemaUpdates.transact(xa).unsafePerformIO
    xa
  }

  def configFromEnv: DatabaseConfig = {
    def env(variable: String, default: Option[String] = None) =
      Option(System.getenv(variable)).orElse(default).getOrElse(sys.error(s"Missing env ${'$' + variable}"))
    DatabaseConfig(
      env("POSTGRES_HOST", Some("localhost")),
      Try(env("POSTGRES_PORT", Some("5432")).toInt).getOrElse(5432),
      env("POSTGRES_DATABASE"),
      env("POSTGRES_USER"),
      env("POSTGRES_PASSWORD")
    )
  }
}

trait Queries {
  import Database._

  def logExecution(e: ExecutionLog): ConnectionIO[Int] =
    sql"""
      INSERT INTO executions (id, job, start_time, end_time, context, success) VALUES
      (${e.id}, ${e.job}, ${e.startTime}, ${e.endTime}, ${e.context}, ${e.success})
    """.update.run

  def getExecutionLog(success: Boolean): ConnectionIO[List[ExecutionLog]] =
    sql"""
      SELECT * FROM executions WHERE success = $success
    """.query[ExecutionLog].list

}
