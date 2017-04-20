package org.criteo.langoustine

import doobie.imports._
import doobie.hikari.imports._

import cats.implicits._

import io.circe._
import io.circe.parser._

import scala.util._

import java.time._

case class DatabaseConfig(host: String, port: Int, database: String, user: String, password: String)

object Database {

  implicit val DateTimeMeta: Meta[LocalDateTime] = Meta[java.sql.Timestamp].nxmap(
    x => LocalDateTime.ofInstant(Instant.ofEpochMilli(x.getTime()), ZoneOffset.of("Z")),
    x => new java.sql.Timestamp(x.toInstant(ZoneOffset.of("Z")).toEpochMilli)
  )

  implicit val JsonMeta: Meta[Json] = Meta[String].nxmap(
    x => parse(x).fold(e => throw e, identity),
    x => x.noSpaces
  )

  val schemaEvolutions = List(
    sql"""
      CREATE TABLE executions (
        id          CHAR(36) NOT NULL,
        job         VARCHAR(255) NOT NULL,
        start_time  DATETIME NOT NULL,
        end_time    DATETIME NOT NULL,
        context     JSON NOT NULL,
        success     BOOLEAN NOT NULL,
        UNIQUE      (id)
      )
    """
  )

  private val doSchemaUpdates =
    (for {
      _ <- sql"""
        CREATE TABLE IF NOT EXISTS schema_evolutions (
          schema_version  SMALLINT NOT NULL,
          schema_update   DATETIME NOT NULL,
          UNIQUE          (schema_version)
        )
      """.update.run

      currentSchemaVersion <- sql"""
        SELECT MAX(schema_version) FROM schema_evolutions
      """.query[Option[Int]].unique.map(_.getOrElse(0))

      _ <- schemaEvolutions.map(_.update).drop(currentSchemaVersion).zipWithIndex.foldLeft(NoUpdate) {
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
        "com.mysql.cj.jdbc.Driver",
        s"jdbc:mysql://${c.host}:${c.port}/${c.database}?serverTimezone=UTC&useSSL=false",
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
      env("MYSQL_HOST", Some("localhost")),
      Try(env("MYSQL_PORT", Some("3306")).toInt).getOrElse(3306),
      env("MYSQL_DATABASE"),
      env("MYSQL_USER"),
      env("MYSQL_PASSWORD")
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
