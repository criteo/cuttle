package com.criteo.cuttle

import java.time.Instant
import java.time.temporal.ChronoUnit

import cats.effect.IO
import com.criteo.cuttle.Auth.User
import doobie.implicits._
import doobie.scalatest.IOChecker

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

  test("should validate paused jobs queries") {
    Database.reset()
    val xa = Database.connect(dbConfig)
    val id = "id1"
    val job = Job(id, testScheduling) { _ =>
      Future.successful(Completed)
    }

    val pausedJob = PausedJob(job.id, User("user1"), Instant.now().truncatedTo(ChronoUnit.SECONDS))

    assert(Queries.pauseJob(pausedJob).transact(xa).unsafeRunSync() == 1)
    assert(Queries.getPausedJobs.transact(xa).unsafeRunSync() == Seq(pausedJob))
  }
}
