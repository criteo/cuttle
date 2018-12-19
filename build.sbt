val devMode = settingKey[Boolean]("Some build optimization are applied in devMode.")
val writeClasspath = taskKey[File]("Write the project classpath to a file.")

val VERSION = "0.5.0"

lazy val catsCore = "1.5.0"
lazy val circe = "0.10.1"
lazy val doobie = "0.6.0"
lazy val lolhttp = "0.12.0"

lazy val commonSettings = Seq(
  organization := "com.criteo.cuttle",
  version := VERSION,
  scalaVersion := "2.11.11",
  crossScalaVersions := Seq("2.11.11", "2.12.3"),
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
    "-Ywarn-unused-import",
    "-Ypartial-unification"
  ) ++ (CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, 12)) => Seq("-Ywarn-unused:-params")
    case _             => Nil
  }),
  devMode := Option(System.getProperty("devMode")).isDefined,
  writeClasspath := {
    val f = file(s"/tmp/classpath_${organization.value}.${name.value}")
    val classpath = (fullClasspath in Runtime).value
    IO.write(f, classpath.map(_.data).mkString(":"))
    streams.value.log.info(f.getAbsolutePath)
    f
  },
  // test config
  testOptions in IntegrationTest := Seq(Tests.Filter(_ endsWith "ITest"), Tests.Argument("-oF")),
  // Maven config
  credentials += Credentials(
    "Sonatype Nexus Repository Manager",
    "oss.sonatype.org",
    "criteo-oss",
    sys.env.getOrElse("SONATYPE_PASSWORD", "")
  ),
  publishTo := Some(
    if (isSnapshot.value)
      Opts.resolver.sonatypeSnapshots
    else
      Opts.resolver.sonatypeStaging
  ),
  pgpPassphrase := sys.env.get("SONATYPE_PASSWORD").map(_.toArray),
  pgpSecretRing := file(".travis/secring.gpg"),
  pgpPublicRing := file(".travis/pubring.gpg"),
  pomExtra in Global := {
    <url>https://github.com/criteo/cuttle</url>
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      </license>
    </licenses>
    <scm>
      <connection>scm:git:github.com/criteo/cuttle.git</connection>
      <developerConnection>scm:git:git@github.com:criteo/cuttle.git</developerConnection>
      <url>github.com/criteo/cuttle</url>
    </scm>
    <developers>
      <developer>
        <name>Guillaume Bort</name>
        <email>g.bort@criteo.com</email>
        <url>https://github.com/guillaumebort</url>
        <organization>Criteo</organization>
        <organizationUrl>http://www.criteo.com</organizationUrl>
      </developer>
      <developer>
        <name>Adrien Surée</name>
        <email>a.suree@criteo.com</email>
        <url>https://github.com/haveo</url>
        <organization>Criteo</organization>
        <organizationUrl>http://www.criteo.com</organizationUrl>
      </developer>
      <developer>
        <name>Justin Coffey</name>
        <email>j.coffey@criteo.com</email>
        <url>https://github.com/jqcoffey</url>
        <organization>Criteo</organization>
        <organizationUrl>http://www.criteo.com</organizationUrl>
      </developer>
      <developer>
        <name>Vincent Guerci</name>
        <email>v.guerci@criteo.com</email>
        <url>https://github.com/vguerci</url>
        <organization>Criteo</organization>
        <organizationUrl>http://www.criteo.com</organizationUrl>
      </developer>
      <developer>
        <name>Alexandre Careil</name>
        <email>a.careil@criteo.com</email>
        <url>https://github.com/hhalex</url>
        <organization>Criteo</organization>
        <organizationUrl>http://www.criteo.com</organizationUrl>
      </developer>
      <developer>
        <name>Arnaud Dufranne</name>
        <email>a.dufranne@criteo.com</email>
        <url>https://github.com/dufrannea</url>
        <organization>Criteo</organization>
        <organizationUrl>http://www.criteo.com</organizationUrl>
      </developer>
      <developer>
        <name>Alexey Eryshev</name>
        <email>a.eryshev@criteo.com</email>
        <url>https://github.com/eryshev</url>
        <organization>Criteo</organization>
        <organizationUrl>http://www.criteo.com</organizationUrl>
      </developer>
      <developer>
        <name>Jean-Philippe Lam Yee Mui</name>
        <email>jp.lamyeemui@criteo.com</email>
        <url>https://github.com/Masuzu</url>
        <organization>Criteo</organization>
        <organizationUrl>http://www.criteo.com</organizationUrl>
      </developer>
      <developer>
        <name>Jean-Baptiste Catté</name>
        <email>jb.catte@criteo.com</email>
        <url>https://github.com/jbkt</url>
        <organization>Criteo</organization>
        <organizationUrl>http://www.criteo.com</organizationUrl>
      </developer>
    </developers>
  },
  // Useful to run flakey tests
  commands += Command.single("repeat") { (state, arg) =>
    arg :: s"repeat $arg" :: state
  },
  // Run an example in another JVM, and quit on key press
  commands += Command.single("example") { (state, arg) =>
    s"examples/test:runMain com.criteo.cuttle.examples.TestExample $arg" :: state
  }
)

def removeDependencies(groups: String*)(xml: scala.xml.Node) = {
  import scala.xml._
  import scala.xml.transform._
  (new RuleTransformer(
    new RewriteRule {
      override def transform(n: Node): Seq[Node] = n match {
        case dependency @ Elem(_, "dependency", _, _, _*) =>
          if (dependency.child.collect { case e: Elem => e }.headOption.exists { e =>
                groups.exists(group => e.toString == s"<groupId>$group</groupId>")
              }) Nil
          else dependency
        case x => x
      }
    }
  ))(xml)
}

lazy val localdb = {
  (project in file("localdb"))
    .settings(commonSettings: _*)
    .settings(
      publishArtifact := false,
      libraryDependencies ++= Seq(
        "com.wix" % "wix-embedded-mysql" % "2.1.4"
      )
    )
}

lazy val cuttle =
  (project in file("core"))
    .configs(IntegrationTest)
    .settings(commonSettings: _*)
    .settings(Defaults.itSettings: _*)
    .settings(
      libraryDependencies ++= Seq(
        "com.criteo.lolhttp" %% "lolhttp",
        "com.criteo.lolhttp" %% "loljson",
        "com.criteo.lolhttp" %% "lolhtml"
      ).map(_ % lolhttp),
      libraryDependencies ++= Seq("core", "generic", "parser", "java8")
        .map(module => "io.circe" %% s"circe-$module" % circe),
      libraryDependencies ++= Seq(
        "de.sciss" %% "fingertree" % "1.5.2",
        "org.scala-stm" %% "scala-stm" % "0.8",
        "org.scala-lang" % "scala-reflect" % scalaVersion.value,
        "org.typelevel" %% "cats-core" % catsCore,
        "codes.reactive" %% "scala-time" % "0.4.1",
        "com.zaxxer" % "nuprocess" % "1.1.0",
        "mysql" % "mysql-connector-java" % "6.0.6"
      ),
      libraryDependencies ++= Seq(
        "org.tpolecat" %% "doobie-core",
        "org.tpolecat" %% "doobie-hikari"
      ).map(_ % doobie),
      libraryDependencies ++= Seq(
        "org.scalatest" %% "scalatest" % "3.0.1",
        "org.mockito" % "mockito-all" % "1.10.19",
        "org.tpolecat" %% "doobie-scalatest" % doobie
      ).map(_ % "it,test")
    )

lazy val timeseries =
  (project in file("timeseries"))
    .settings(commonSettings: _*)
    .settings(
      libraryDependencies ++= Seq(
        "com.wix" % "wix-embedded-mysql" % "2.1.4" % "test"
      ))
    .settings(
      // Webpack
      resourceGenerators in Compile += Def.task {
        import scala.sys.process._
        val streams0 = streams.value
        val webpackOutputDir: File = (resourceManaged in Compile).value / "public"
        if (devMode.value) {
          streams0.log.warn(s"Skipping webpack resource generation.")
          Nil
        } else {
          def listFiles(dir: File): Seq[File] =
            IO.listFiles(dir)
              .flatMap(f =>
                if (f.isDirectory) listFiles(f)
                else Seq(f))
          val logger = new ProcessLogger {
            override def err(s: => String): Unit = streams0.log.info(s"ERR, $s")
            override def buffer[T](f: => T): T = f
            override def out(s: => String): Unit = streams0.log.info(s)
          }
          logger.out(s"Generating UI assets to $webpackOutputDir...")
          val operatingSystem = System.getProperty("os.name").toLowerCase
          if (operatingSystem.indexOf("win") >= 0) {
            val yarnJsPath = ("where yarn.js" !!).trim()
            assert(s"""node "$yarnJsPath" install""" ! logger == 0, "yarn failed")
          } else {
            assert("yarn install" ! logger == 0, "yarn failed")
          }
          logger.out("Running webpack...")
          assert(s"node node_modules/webpack/bin/webpack.js --output-path $webpackOutputDir --bail" ! logger == 0,
                 "webpack failed")
          listFiles(webpackOutputDir)
        }
      }.taskValue,
      cleanFiles += (file(".") / "node_modules")
    )
    .dependsOn(cuttle % "compile->compile;test->test")

lazy val cron =
  (project in file("cron"))
    .settings(commonSettings: _*)
    .settings(
      libraryDependencies += "com.github.alonsodomin.cron4s" %% "cron4s-core" % "0.4.5"
    )
    .dependsOn(cuttle % "compile->compile;test->test")

lazy val examples =
  (project in file("examples"))
    .settings(commonSettings: _*)
    .settings(
      publishArtifact := false,
      fork in Test := true,
      connectInput in Test := true,
      javaOptions ++= Seq("-Xmx256m", "-XX:+HeapDumpOnOutOfMemoryError"),
    )
    .settings(
      Option(System.getProperty("generateExamples"))
        .map(_ =>
          Seq(
            autoCompilerPlugins := true,
            addCompilerPlugin("com.criteo.socco" %% "socco-plugin" % "0.1.7"),
            scalacOptions := Seq(
              "-P:socco:out:examples/target/html",
              "-P:socco:package_com.criteo.cuttle:https://criteo.github.io/cuttle/api/"
            )
        ))
        .getOrElse(Nil): _*
    )
    .dependsOn(cuttle, timeseries, cron, localdb)

lazy val root =
  (project in file("."))
    .enablePlugins(ScalaUnidocPlugin)
    .settings(commonSettings: _*)
    .settings(
      publishArtifact := false,
      scalacOptions in (ScalaUnidoc, unidoc) ++= Seq(
        Seq(
          "-sourcepath",
          baseDirectory.value.getAbsolutePath
        ),
        Opts.doc.title("cuttle"),
        Opts.doc.version(VERSION),
        Opts.doc.sourceUrl("https://github.com/criteo/cuttle/blob/master€{FILE_PATH}.scala"),
        Seq(
          "-doc-root-content",
          (baseDirectory.value / "core/src/main/scala/root.scala").getAbsolutePath
        )
      ).flatten,
      unidocAllAPIMappings in (ScalaUnidoc, unidoc) ++= {
        val allJars = {
          (fullClasspath in cuttle in Compile).value ++
            (fullClasspath in timeseries in Compile).value ++
            (fullClasspath in cron in Compile).value
        }
        Seq(
          allJars
            .flatMap(x => x.metadata.get(moduleID.key).map(m => x.data -> m))
            .collect {
              case (jar, module) if module.name == "scala-library" =>
                jar -> url("https://www.scala-lang.org/api/current/")
              case (jar, module) if module.name.contains("doobie") =>
                jar -> url("https://www.javadoc.io/doc/org.tpolecat/doobie-core_2.12/0.4.1/")
              case (jar, module) if module.name.contains("lolhttp") =>
                jar -> url("https://criteo.github.io/lolhttp/api/")
              case (jar, module) if module.name.contains("circe") =>
                jar -> url("http://circe.github.io/circe/api/")
            }
            .toMap
        )
      },
      unidocProjectFilter in (ScalaUnidoc, unidoc) := inProjects(cuttle, timeseries, cron)
    )
    .aggregate(cuttle, timeseries, cron, examples, localdb)
