package com.criteo.cuttle

import java.time.{Instant, LocalDateTime, ZoneOffset}
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
  override val transactor: doobie.Transactor[IO] = Database.newHikariTransactor(dbConfig).allocated.unsafeRunSync()._1

  test("should establish the connection and instanciate a trasactor") {
    assert(Database.connect(dbConfig).isInstanceOf[doobie.Transactor[IO]])
  }

  test("should validate getPausedJobIdsQuery") {
    Database.reset()
    Database.connect(dbConfig)
    check(queries.getPausedJobIdsQuery)
  }

  test("should validate paused jobs queries") {
    Database.reset()
    val xa = Database.connect(dbConfig)
    val id = "id1"
    val job = Job(id, testScheduling) { _ =>
      Future.successful(Completed)
    }

    val pausedJob = PausedJob(job.id, User("user1"), Instant.now().truncatedTo(ChronoUnit.SECONDS))

    assert(queries.pauseJob(pausedJob).transact(xa).unsafeRunSync() == 1)
    assert(queries.getPausedJobs.transact(xa).unsafeRunSync() == Seq(pausedJob))
  }

  test("paused_jobs migration(1) should set default values for old pauses") {
    Database.reset()

    Database.schemaEvolutions.head.transact(transactor).unsafeRunSync()
    sql"INSERT INTO paused_jobs VALUES ('1')".update.run.transact(transactor).unsafeRunSync()
    val id = sql"SELECT * FROM paused_jobs".query[String].unique.transact(transactor).unsafeRunSync()
    assert(id == "1")

    Database.schemaEvolutions(1).transact(transactor).unsafeRunSync()

    val pausedJob = sql"SELECT * FROM paused_jobs".query[PausedJob].unique.transact(transactor).unsafeRunSync()
    assert(pausedJob.id == "1")
    assert(pausedJob.user == User("not defined user"))
    assert(pausedJob.date == LocalDateTime.parse("1991-11-01T15:42:00").toInstant(ZoneOffset.UTC))
  }

  test("we should be able to retrieve finished executions") {
    Database.reset()

    Database.doSchemaUpdates.transact(transactor).unsafeRunSync()
    val ctx = TestContext()
    val date = Some(Instant.now())
    val el = ExecutionLog("id", "hello", date, date, ctx.asJson, ExecutionStatus.ExecutionSuccessful, None, 10)
    (0 to 100).foreach { i =>
      queries.logExecution(el.copy(s"${el.id}-$i"), ctx.logIntoDatabase).transact(transactor).unsafeRunSync()
      val executionLogSize = queries.getExecutionLogSize(Set("hello")).transact(transactor).unsafeRunSync()
      assert(executionLogSize == i + 1)
    }
  }
}
