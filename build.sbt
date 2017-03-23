name := "langoustinepp"
organization := "com.criteo"
version := "0.1.0"
scalaVersion := "2.11.8"

mainClass in(Compile, run) := Some("com.criteo.langoustinepp.LangoustinePP")

libraryDependencies ++= Seq(
  "com.criteo.lolhttp" %% "lolhttp" % "0.1.0",
  "com.criteo.lolhttp" %% "loljson" % "0.1.0"
)

resolvers += "Nexus" at "http://nexus.criteo.prod/content/repositories/criteo.thirdparty/"

resourceGenerators in Compile += Def.task {
  def listFiles(dir: File): Seq[File] = {
    IO.listFiles(dir).flatMap(f =>
      if (f.isDirectory) listFiles(f)
      else Seq(f)
    )
  }
  val webpackOutputDir: File = (resourceManaged in Compile).value / "public"
  val logger = new ProcessLogger {
    override def error(s: =>String): Unit = ()
    override def buffer[T](f: => T): T = f
    override def info(s: => String): Unit = streams.value.log.info(s)
  }
  logger.info(s"Generating UI assets to $webpackOutputDir...")
  assert("yarn install" ! logger == 0, "yarn failed")
  logger.info("Running webpack...")
  assert(s"./node_modules/webpack/bin/webpack.js --output-path $webpackOutputDir --bail" ! logger == 0, "webpack failed")
  listFiles(webpackOutputDir)
}.taskValue
