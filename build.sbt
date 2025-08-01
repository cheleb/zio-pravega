name := "ZIO Pravega"
val scala213  = "2.13.16"
val scala33   = "3.6.3"
val mainScala = scala33
val allScala  = Seq(scala33, scala213)

val zioVersion     = "2.1.20"
val pravegaVersion = "0.13.0"

def scalacOptionsFor(scalaVersion: String): Seq[String] =
  CrossVersion.partialVersion(scalaVersion) match {
    case Some((3, _)) => Seq("-Wunused:all")
    case _            => Seq("-Xsource:3")
  }

inThisBuild(
  List(
    organization := "dev.cheleb",
    homepage     := Some(url("https://github.com/cheleb/zio-pravega")),
    licenses := List(
      "Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")
    ),
    useCoursier        := false,
    scalaVersion       := mainScala,
    crossScalaVersions := allScala,
    scalacOptions ++= Seq(
//      "-deprecation",
      "-Xfatal-warnings"
    ),
    Test / parallelExecution := false,
    Test / fork              := true,
    run / fork               := true,
    publishTo := {
      val centralSnapshots =
        "https://central.sonatype.com/repository/maven-snapshots/"
      if (isSnapshot.value) Some("central-snapshots" at centralSnapshots)
      else localStaging.value
    },
    versionScheme := Some("early-semver"),
    pgpPublicRing := file("/tmp/public.asc"),
    pgpSecretRing := file("/tmp/secret.asc"),
    pgpPassphrase := sys.env.get("PGP_PASSWORD").map(_.toArray),
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/cheleb/zio-pravega/"),
        "scm:git:git@github.com:cheleb/zio-pravega.git"
      )
    ),
    developers := List(
      Developer(
        "cheleb",
        "Olivier NOUGUIER",
        "olivier.nouguier@gmail.com",
        url("https://github.com/cheleb")
      )
    ),
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision, // only required for Scala 2.x
    //  scalafixScalaBinaryVersion := "3.4",
    //  scalafixOnCompile          := true,
    ThisBuild / scalafixDependencies +=
      "dev.cheleb" %% "zio-module-pattern" % "0.0.5",
    Compile / wartremoverErrors ++= Warts.allBut(
      Wart.Any,
      Wart.Nothing,
      Wart.DefaultArguments,
      Wart.ImplicitParameter,
      Wart.Overloading
    )
  )
)

lazy val root = project
  .in(file("."))
  .aggregate(pravega)
  .settings(
    publish / skip := true
  )

lazy val pravega =
  project
    .in(file("modules/zio-pravega"))
    .enablePlugins(BuildInfoPlugin)
    .settings(
      name              := "zio-pravega",
      scalafmtOnCompile := true,
      fork              := true,
      Global / scalacOptions ++= scalacOptionsFor(scalaVersion.value)
    )
    .settings(
      buildInfoKeys := Seq[BuildInfoKey](
        organization,
        name,
        version,
        scalaVersion,
        sbtVersion,
        isSnapshot
      ),
      buildInfoPackage := "zio.pravega"
    )
    .settings(
      resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
//      resolvers += "Pravega RC" at "https://oss.sonatype.org/content/repositories/iopravega-1206",
      libraryDependencies ++= Seq(
        "com.typesafe"                      % "config"                   % "1.4.4",
        "dev.zio"                          %% "zio-streams"              % zioVersion,
        "dev.zio"                          %% "zio-test"                 % zioVersion                              % Test,
        "dev.zio"                          %% "zio-test-sbt"             % zioVersion                              % Test,
        "dev.zio"                          %% "zio-logging-slf4j-bridge" % "2.5.1"                                 % Test,
        "org.scalatest"                    %% "scalatest"                % "3.2.19"                                % Test,
        "io.pravega"                        % "pravega-client"           % pravegaVersion,
        "org.testcontainers"                % "testcontainers"           % "1.21.3"                                % Test,
        "org.scala-lang.modules"           %% "scala-collection-compat"  % "2.13.0",
        "com.thesamet.scalapb"             %% "scalapb-runtime"          % scalapb.compiler.Version.scalapbVersion % Test,
        "io.envoyproxy.protoc-gen-validate" % "pgv-java-stub"            % "0.6.13"                                % Test,
        "com.thesamet.scalapb"             %% "scalapb-runtime"          % scalapb.compiler.Version.scalapbVersion % "protobuf"
      ),
      testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
    )
    .settings(
      libraryDependencies ++= Seq(
        "com.thesamet.scalapb.common-protos" %% "proto-google-common-protos-scalapb_0.11" % "2.9.6-0"  % "protobuf",
        "com.thesamet.scalapb.common-protos" %% "proto-google-common-protos-scalapb_0.11" % "2.9.6-0"  % Test,
        "com.thesamet.scalapb.common-protos" %% "pgv-proto-scalapb_0.11"                  % "0.6.13-0" % "protobuf",
        "com.thesamet.scalapb.common-protos" %% "pgv-proto-scalapb_0.11"                  % "0.6.13-0" % Test
      ),
      Test / PB.targets := Seq(
        scalapb.gen(scala3Sources = true) -> (Test / sourceManaged).value / "scalapb"
      )
    )

lazy val saga = project
  .in(file("modules/zio-pravega-saga"))
  .dependsOn(pravega)
  .settings(
    name := "zio-pravega-saga"
  )
  .settings(
    publish / skip := true
  )

lazy val docs = project // new documentation project
  .in(file("zio-pravega-docs")) // important: it must not be docs/
  .dependsOn(pravega)
  .settings(
    publish / skip                             := true,
    moduleName                                 := "zio-pravega-docs",
    ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(pravega),
    ScalaUnidoc / unidoc / target              := (LocalRootProject / baseDirectory).value / "website" / "static" / "api",
    cleanFiles += (ScalaUnidoc / unidoc / target).value,
    mdocVariables := Map(
      "VERSION" -> sys.env.getOrElse("VERSION", version.value),
      "ORG"     -> organization.value
    )
  )
  .disablePlugins(WartRemover)
  .enablePlugins(MdocPlugin, ScalaUnidocPlugin, PlantUMLPlugin)
  .settings(
    plantUMLSource           := (root / baseDirectory).value / "docs" / "_docs",
    Compile / plantUMLTarget := "mdoc/_assets/images"
  )
  .settings(libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.5.18")

addCommandAlias("fmt", "all scalafmtSbt scalafmt test / scalafmt")
addCommandAlias(
  "check",
  "all scalafmtSbtCheck scalafmtCheck test / scalafmtCheck"
)
