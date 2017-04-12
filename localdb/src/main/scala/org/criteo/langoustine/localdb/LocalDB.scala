package org.criteo.langoustine.localdb

import com.wix.mysql._
import com.wix.mysql.config._
import com.wix.mysql.distribution.Version._
import com.wix.mysql.config.Charset._

object LocalDB {
  def main(args: Array[String]): Unit = {
    val config = {
      MysqldConfig.aMysqldConfig(v5_7_latest).withCharset(UTF8).withPort(3388).build()
    }
    val mysqld = EmbeddedMysql.anEmbeddedMysql(config).addSchema("langoustine_dev").start()
    println("if needed you can connect to this running db using:")
    println("> mysql -u root -h 127.0.0.1 -P 3388 langoustine_dev")
    println("press [Ctrl+D] to stop...")
    while (System.in.read != -1) ()
    mysqld.stop()
  }
}
