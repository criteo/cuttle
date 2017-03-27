val devMode = Option(System.getProperty("devMode")).exists(_ == "true")

name := "langoustinepp"
organization := "com.criteo"
version := "0.1.0"
scalaVersion := "2.12.1"

scalacOptions := Seq("-feature", "-deprecation")

mainClass in(Compile, run) := Some("com.criteo.langoustinepp.LangoustinePP")

libraryDependencies ++= Seq(
  "org.criteo.lolhttp" %% "lolhttp" % "0.2.2",
  "org.criteo.lolhttp" %% "loljson" % "0.2.2",
  "org.scala-stm" %% "scala-stm" % "0.8",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.scalacheck" %% "scalacheck" % "1.13.4" % "provided",
  "codes.reactive" %% "scala-time" % "0.4.1"
)

resolvers += "Nexus" at "http://nexus.criteo.prod/content/repositories/criteo.thirdparty/"

resourceGenerators in Compile += Def.task {
  if(devMode) Nil else {
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
  }
}.taskValue
