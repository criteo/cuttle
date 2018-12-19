package com.criteo.cuttle

import cats.effect.IO
import doobie.implicits._
import doobie.util.log
import org.scalatest.{BeforeAndAfter, FunSuite}

class DatabaseSuite extends FunSuite with BeforeAndAfter {
  val dbName = "cuttle_it_test"

  val queries: Queries = new Queries {}

  private val dbConfig = DatabaseConfig(
    Seq(DBLocation("localhost", 3388)),
    "sys",
    "root",
    ""
  )

  // service transactor is used for schema creation
  private val serviceTransactor: doobie.Transactor[IO] = Database.newHikariTransactor(dbConfig)
    .allocated.unsafeRunSync()._1

  private implicit val logHandler: log.LogHandler = DoobieLogsHandler(logger).handler

  private def createDatabaseIfNotExists(): Unit =
    sql"CREATE DATABASE IF NOT EXISTS cuttle_it_test".update.run.transact(serviceTransactor).unsafeRunSync()

  private def clean(): Unit =
    sql"DROP DATABASE IF EXISTS cuttle_it_test".update.run.transact(serviceTransactor).unsafeRunSync()

  before {
    clean()
    createDatabaseIfNotExists()
  }
}
