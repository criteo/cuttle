package com.criteo.cuttle.cron

import java.time.Instant

import cats._
import cats.implicits._
import com.criteo.cuttle.utils
import com.criteo.cuttle.Database.JsonMeta
import doobie._
import doobie.implicits._
import io.circe._
import io.circe.parser._
import io.circe.java8.time._

private[cron] object Database {

  private val migrateContexts: ConnectionIO[Unit] = {
    val chunkSize = 1024 * 10
    val insertTmp = Update[(String, String)]("INSERT into tmp (old_context_id, new_context_id) VALUES (? , ?)")
    val insertContext =
      Update[(String, Json, Instant)]("REPLACE into cron_contexts (id, json, instant) VALUES (? , ?, ?)")
    for {
      _ <- sql"""
       CREATE TABLE cron_contexts (
         id VARCHAR(1000) NOT NULL,
         json JSON NOT NULL,
         instant DATETIME(3) NOT NULL,
         PRIMARY KEY(id)
       ) ENGINE = INNODB;

       CREATE INDEX cron_contexts_by_instant ON cron_contexts (instant);
       """.update.run
      _ <- sql"""CREATE TEMPORARY TABLE tmp (old_context_id VARCHAR(1000), new_context_id VARCHAR(1000))""".update.run
      _ <- sql"""SELECT DISTINCT context_id FROM executions"""
        .query[String]
        .streamWithChunkSize(chunkSize)
        .chunkLimit(chunkSize)
        .evalMap { oldContexts =>
          val execIdOldNewContext = oldContexts.map { oldContextId =>
            // Convert old context {"interval": xxx, ...} to new context with only the interval value named "instant"
            parse(oldContextId).flatMap(_.hcursor.downField("interval").as[Instant]) match {
              case Left(_)        => (oldContextId, CronContext(Instant.EPOCH))
              case Right(instant) => (oldContextId, CronContext(instant))
            }
          }
          (insertTmp.updateMany(execIdOldNewContext.map {
            case (oldContextId, newContext) =>
              (oldContextId, newContext.longRunningId())
          }), insertContext.updateMany(execIdOldNewContext.map {
            case (_, newContext) =>
              (newContext.longRunningId(), newContext.asJson, newContext.instant)
          })).mapN(_ + _)
        }
        .compile
        .drain
      _ <- sql"""CREATE INDEX tmp_id ON tmp (old_context_id)""".update.run
      _ <- sql"""
          UPDATE executions exec
          JOIN tmp ON exec.context_id = tmp.old_context_id
          SET exec.context_id = tmp.new_context_id
        """.update.run
    } yield ()
  }

  val schema = List(
    migrateContexts
  )

  val doSchemaUpdates: ConnectionIO[Unit] = utils.updateSchema("cron_schema_evolutions", schema)

  def sqlGetContextsBetween(start: Option[Instant], end: Option[Instant]): Fragment = {
    val where: Fragment = (start, end) match {
      case (Some(s), Some(e)) => sql"WHERE instant BETWEEN $s AND $e"
      case (Some(s), None)    => sql"WHERE instant >= $s"
      case (None, Some(e))    => sql"WHERE instant <= $e"
      case (None, None)       => Fragment.empty
    }
    sql"SELECT id, json FROM cron_contexts" ++ where
  }

  def serializeContext(context: CronContext): ConnectionIO[String] = {
    val id = context.longRunningId()
    sql"""
      REPLACE INTO cron_contexts (id, json, instant)
      VALUES (
        $id,
        ${context.asJson},
        ${context.instant}
      )
    """.update.run *> Applicative[ConnectionIO].pure(id)
  }
}
