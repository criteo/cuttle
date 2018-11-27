package com.criteo.cuttle.timeseries.contrib

import doobie.implicits._
import com.criteo.cuttle._
import java.time._

class PersistInstant(xa: XA) {
  def set(id: String, t: Instant): Instant = {
    sql"REPLACE INTO instant_data VALUES (${id}, ${t})".update.run.transact(xa).unsafeRunSync
    t
  }

  def get(id: String): Option[Instant] =
    sql"SELECT instant FROM instant_data WHERE id = ${id}"
      .query[Instant]
      .option
      .transact(xa)
      .unsafeRunSync
}

object PersistInstant {
  private val schemaUpgrades = List(
    sql"""
      CREATE TABLE instant_data (
        id        VARCHAR(1000) NOT NULL,
        instant   DATETIME NOT NULL,
        PRIMARY KEY (id)
      ) ENGINE = INNODB
    """.update.run
  )

  def apply(xa: XA): PersistInstant = {
    utils.updateSchema("instant", schemaUpgrades).transact(xa).unsafeRunSync
    new PersistInstant(xa)
  }
}
