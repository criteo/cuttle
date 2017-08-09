val devMode = settingKey[Boolean]("Some build optimization are applied in devMode.")
val writeClasspath = taskKey[File]("Write the project classpath to a file.")

val VERSION = "0.1.10"

lazy val commonSettings = Seq(
  organization := "com.criteo.cuttle",
  version := VERSION,
  scalaVersion := "2.11.11",
  crossScalaVersions := Seq("2.11.11", "2.12.2"),
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
  credentials += Credentials(
    "Sonatype Nexus Repository Manager",
    "oss.sonatype.org",
    "criteo-oss",
    sys.env.getOrElse("SONATYPE_PASSWORD", "")
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
        <name>Justin coffey</name>
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
        case dependency @ Elem(_, "dependency", _, _, _ *) =>
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
    .settings(commonSettings: _*)
    .settings(
      libraryDependencies ++= Seq(
        "com.criteo.lolhttp" %% "lolhttp",
        "com.criteo.lolhttp" %% "loljson",
        "com.criteo.lolhttp" %% "lolhtml"
      ).map(_ % "0.5.1"),
      libraryDependencies ++= Seq("core", "generic", "parser")
        .map(module => "io.circe" %% s"circe-${module}" % "0.7.1"),
      libraryDependencies ++= Seq(
        "de.sciss" %% "fingertree" % "1.5.2",
        "org.scala-stm" %% "scala-stm" % "0.8",
        "org.scala-lang" % "scala-reflect" % "2.11.9",
        "org.typelevel" %% "cats" % "0.9.0",
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
      // Webpack
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
      publishArtifact := false,
      fork in Test := true,
      connectInput in Test := true
    )
    .dependsOn(cuttle, timeseries, localdb)

lazy val root =
  (project in file("."))
    .enablePlugins(ScalaUnidocPlugin)
    .settings(commonSettings: _*)
    .settings(
      publishArtifact := false,
      scalacOptions in (Compile, doc) ++= Seq(
        Seq(
          "-sourcepath",
          baseDirectory.value.getAbsolutePath
        ),
        Opts.doc.title("cuttle"),
        Opts.doc.version(VERSION),
        Opts.doc.sourceUrl("https://github.com/criteo/cuttle/blob/master€{FILE_PATH}.scala")
      ).flatten,
      // Not so useful for now because of SI-9967
      unidocAllAPIMappings in (ScalaUnidoc, unidoc) ++= {
        val allJars = {
          (fullClasspath in cuttle in Compile).value ++
            (fullClasspath in timeseries in Compile).value
        }
        Seq(
          allJars
            .flatMap(x => x.metadata.get(moduleID.key).map(m => x.data -> m))
            .collect {
              case (jar, ModuleID("org.scala-lang", "scala-library", _, _, _, _, _, _, _, _, _)) =>
                jar -> "https://www.scala-lang.org/api/current/"
            }
            .toMap
            .mapValues(url => new java.net.URL(url))
        )
      },
      unidocProjectFilter in (ScalaUnidoc, unidoc) := inProjects(cuttle, timeseries)
    )
    .aggregate(cuttle, timeseries, examples, localdb)
