package com.criteo.cuttle.localdb

import java.util.concurrent.TimeUnit

import com.wix.mysql._
import com.wix.mysql.config._
import com.wix.mysql.distribution.Version._
import com.wix.mysql.config.Charset._

object LocalDB {
  def main(args: Array[String]): Unit = {
    val config = {
      MysqldConfig.aMysqldConfig(v5_7_latest).withCharset(UTF8).withTimeout(3600, TimeUnit.SECONDS).withPort(3388).build()
    }
    val mysqld = EmbeddedMysql.anEmbeddedMysql(config).addSchema("cuttle_dev").start()
    println("started!")
    println("if needed you can connect to this running db using:")
    println("> mysql -u root -h 127.0.0.1 -P 3388 cuttle_dev")
    println("press [Ctrl+D] to stop...")
    while (System.in.read != -1) ()
    mysqld.stop()
  }
}
