package com.criteo.cuttle

import cats.effect.IO
import doobie.scalatest.IOChecker
import doobie.implicits._
import cats.implicits._

import scala.concurrent.Future

class DatabaseITest extends DatabaseSuite with IOChecker with TestScheduling {
  val dbConfig = DatabaseConfig(
    Seq(DBLocation("localhost", 3388)),
    dbName,
    "root",
    ""
  )

  // IOChecker needs a transactor for performing its queries
  override val transactor: doobie.Transactor[IO] = Database.newHikariTransactor(dbConfig)

  test("should establish the connection and instanciate a trasactor") {
    assert(Database.connect(dbConfig).isInstanceOf[doobie.Transactor[IO]])
  }

  test("should validate getPausedJobIdsQuery") {
    Database.reset()
    Database.connect(dbConfig)
    check(Queries.getPausedJobIdsQuery)
  }

  test("should validate query") {
    Database.reset()
    val xa = Database.connect(dbConfig)
    val id = "id1"
    val job = Job(id, testScheduling) { _ =>
      Future.successful(Completed)
    }

    assert(Queries.pauseJob(job).transact(xa).unsafeRunSync() == 1)
    assert(Queries.getPausedJobIds.transact(xa).unsafeRunSync() == Set(id))
  }
}
