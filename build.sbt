val devMode = settingKey[Boolean]("Some build optimization are applied in devMode.")
val writeClasspath = taskKey[File]("Write the project classpath to a file.")

lazy val commonSettings = Seq(
  organization := "com.criteo.cuttle",
  version := "0.1.0",
  scalaVersion := "2.11.9",
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding",
    "UTF-8",
    "-feature",
    "-unchecked",
    "-Xlint",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Xfuture",
    "-Ywarn-unused",
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
  // Useful to run flakey tests
  commands += Command.single("repeat") { (state, arg) =>
    arg :: s"repeat $arg" :: state
  },
  // Run an example in another JVM, and quit on key press
  commands += Command.single("example") { (state, arg) =>
    s"examples/test:runMain com.criteo.cuttle.examples.TestExample $arg" :: state
  }
)

lazy val continuum = {
  ProjectRef(uri("git://github.com/danburkert/continuum.git#b2122f1980fb69eb0d047e47e9d7de730ccbd448"), "continuum")
}

lazy val localdb = {
  (project in file("localdb"))
    .settings(commonSettings: _*)
    .settings(
      libraryDependencies ++= Seq(
        "com.wix" % "wix-embedded-mysql" % "2.1.4"
      )
    )
}

lazy val cuttle =
  (project in file("core"))
    .settings(commonSettings: _*)
    .settings(
      libraryDependencies ++= Seq(
        "com.criteo.lolhttp" %% "lolhttp",
        "com.criteo.lolhttp" %% "loljson",
        "com.criteo.lolhttp" %% "lolhtml"
      ).map(_ % "0.4.2"),
      libraryDependencies ++= Seq("core", "generic", "parser")
        .map(module => "io.circe" %% s"circe-${module}" % "0.7.1"),
      libraryDependencies ++= Seq(
        "org.scala-stm" %% "scala-stm" % "0.8",
        "org.scala-lang" % "scala-reflect" % "2.11.9",
        "org.typelevel" %% "cats" % "0.9.0",
        "org.typelevel" %% "algebra" % "0.7.0",
        "codes.reactive" %% "scala-time" % "0.4.1",
        "com.zaxxer" % "nuprocess" % "1.1.0"
      ),
      libraryDependencies ++= Seq(
        "org.tpolecat" %% "doobie-core-cats",
        "org.tpolecat" %% "doobie-hikari-cats"
      ).map(_ % "0.4.1"),
      libraryDependencies ++= Seq(
        "mysql" % "mysql-connector-java" % "6.0.6"
      ),
      libraryDependencies ++= Seq(
        "org.scalatest" %% "scalatest" % "3.0.1"
      ).map(_ % "test"),
      resourceGenerators in Compile += Def.task {
        if (devMode.value) {
          streams.value.log.warn(s"Skipping webpack resource generation.")
          Nil
        } else {
          def listFiles(dir: File): Seq[File] =
            IO.listFiles(dir)
              .flatMap(f =>
                if (f.isDirectory) listFiles(f)
                else Seq(f))
          val webpackOutputDir: File = (resourceManaged in Compile).value / "public"
          val logger = new ProcessLogger {
            override def error(s: => String): Unit = streams.value.log.info(s"ERR, $s")
            override def buffer[T](f: => T): T = f
            override def info(s: => String): Unit = streams.value.log.info(s)
          }
          logger.info(s"Generating UI assets to $webpackOutputDir...")
          assert(s"yarn install" ! logger == 0, "yarn failed")
          logger.info("Running webpack...")
          assert(s"./node_modules/webpack/bin/webpack.js --output-path $webpackOutputDir --bail" ! logger == 0,
                 "webpack failed")
          listFiles(webpackOutputDir)
        }
      }.taskValue,
      cleanFiles += (file(".") / "node_modules")
    )
    .dependsOn(continuum, localdb % "test->test")

lazy val timeseries =
  (project in file("timeseries"))
    .settings(commonSettings: _*)
    .settings(
      )
    .dependsOn(cuttle % "compile->compile;test->test")

lazy val examples =
  (project in file("examples"))
    .settings(commonSettings: _*)
    .settings(
      fork in Test := true,
      connectInput in Test := true
    )
    .dependsOn(cuttle, timeseries, localdb)

lazy val root =
  (project in file("."))
    .settings(commonSettings: _*)
    .settings(
      publishArtifact := false
    )
    .aggregate(cuttle, timeseries, examples, localdb)
