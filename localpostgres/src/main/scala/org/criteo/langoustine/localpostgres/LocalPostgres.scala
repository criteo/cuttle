package org.criteo.langoustine.localpostgres

import ru.yandex.qatools.embed.postgresql._
import ru.yandex.qatools.embed.postgresql.config._
import ru.yandex.qatools.embed.postgresql.distribution.Version.Main.PRODUCTION

object LocalPostgres {
  def main(args: Array[String]): Unit = {
    val runtime = PostgresStarter.getDefaultInstance()
    val config = new PostgresConfig(
      PRODUCTION,
      new AbstractPostgresConfig.Net("localhost", 5488),
      new AbstractPostgresConfig.Storage("langoustine"),
      new AbstractPostgresConfig.Timeout(),
      new AbstractPostgresConfig.Credentials("root", "secret")
    )
    val pg = runtime.prepare(config).start()
    println(config)
    println("press [Ctrl+D] to stop...")
    while (System.in.read != -1) ()
    pg.stop()
  }
}