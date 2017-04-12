package org.criteo.langoustine

import doobie.imports._
import doobie.hikari.imports._

import scala.util.{Try}

trait Database {
  def run[A](statements: ConnectionIO[A]): A
  def shutdown(): Unit
}

case class DatabaseConfig(host: String, port: Int, database: String, user: String, password: String)

object Database {

  def connect(c: DatabaseConfig): Database = new Database {
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

    def run[A](statements: ConnectionIO[A]) = statements.transact(xa).unsafePerformIO
    def shutdown() = xa.shutdown.unsafePerformIO
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
