val devMode = settingKey[Boolean]("Some build optimization are applied in devMode.")
val writeClasspath = taskKey[File]("Write the project classpath to a file.")

lazy val commonSettings = Seq(
  organization := "org.criteo.langoustine",
  version := "dev-SNAPSHOT",
  scalaVersion := "2.11.9",
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding", "UTF-8",
    "-feature",
    "-unchecked",
    "-Xlint",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-language:postfixOps",
    "-Xfuture",
    "-Ywarn-unused-import"
  ),
  devMode := Option(System.getProperty("devMode")).isDefined,
  writeClasspath := {
    val f = file(s"/tmp/classpath_${organization.value}.${name.value}")
    val classpath = (fullClasspath in Runtime).value
    IO.write(f, classpath.map(_.data).mkString(":"))
    streams.value.log.info(f.getAbsolutePath)
    f
  },

  // Maven config
  resolvers += "Nexus" at "http://nexus.criteo.prod/content/repositories/criteo.thirdparty/",
  publishTo := Some("Criteo thirdparty" at "http://nexus.criteo.prod/content/repositories/criteo.thirdparty"),
  credentials += Credentials("Sonatype Nexus Repository Manager", "nexus.criteo.prod", System.getenv("MAVEN_USER"), System.getenv("MAVEN_PASSWORD")),

  // Useful to run flakey tests
  commands += Command.single("repeat") { (state, arg) =>
    arg :: s"repeat $arg" :: state
  },

  // Run an example in another JVM, and quit on key press
  commands += Command.single("example") { (state, arg) =>
    s"examples/test:runMain org.criteo.langoustine.examples.TestExample $arg" :: state
  }
)

lazy val continuum = {
  ProjectRef(uri("git://github.com/danburkert/continuum.git#b2122f1980fb69eb0d047e47e9d7de730ccbd448"), "continuum")
}

lazy val langoustine =
  (project in file("core")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= Seq(
      "org.criteo.lolhttp" %% "lolhttp",
      "org.criteo.lolhttp" %% "loljson"
    ).map(_ % "0.2.2"),

    libraryDependencies ++= Seq(
      "org.scala-stm" %% "scala-stm" % "0.8",
      "org.scala-lang" % "scala-reflect" % "2.11.9",
      "codes.reactive" %% "scala-time" % "0.4.1",
      "com.zaxxer" % "nuprocess" % "1.1.0"
    ),

    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.0.1"
    ).map(_ % "test"),

    resourceGenerators in Compile += Def.task {
      if(devMode.value) {
        streams.value.log.warn(s"Skipping webpack resource generation.")
        Nil
      } else {
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
        assert(s"yarn install" ! logger == 0, "yarn failed")
        logger.info("Running webpack...")
        assert(s"./node_modules/webpack/bin/webpack.js --output-path $webpackOutputDir --bail" ! logger == 0, "webpack failed")
        listFiles(webpackOutputDir)
      }
    }.taskValue
  ).
  dependsOn(continuum)

lazy val timeseries =
  (project in file("timeseries")).
  settings(commonSettings: _*).
  settings(
  ).
  dependsOn(langoustine % "compile->compile;test->test")

lazy val examples =
  (project in file("examples")).
  settings(commonSettings: _*).
  settings(
    fork in Test := true,
    connectInput in Test := true
  ).
  dependsOn(langoustine, timeseries)

lazy val root =
  (project in file(".")).
  settings(commonSettings: _*).
  settings(
    publishArtifact := false
  ).
  aggregate(langoustine, timeseries, examples)
