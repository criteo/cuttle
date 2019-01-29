package com.criteo.cuttle.localdb

import ch.vorburger.mariadb4j._

object LocalDB {
  def main(args: Array[String]): Unit = {
    val config = DBConfigurationBuilder.newBuilder()
    config.setPort(3388)
    config.setDatabaseVersion("mariadb-10.2.11")
    val db = DB.newEmbeddedDB(config.build)
    db.start()
    db.createDB("cuttle_dev")
    println("started!")
    println("if needed you can connect to this running db using:")
    println("> mysql -u root -h 127.0.0.1 -P 3388 cuttle_dev")
    println("press [Ctrl+D] to stop...")
    while (System.in.read != -1) ()
    db.stop()
  }
}
