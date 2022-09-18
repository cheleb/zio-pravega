val scala213 = "2.13.8"
val scala32 = "3.2.0"
val mainScala = scala213
val allScala = Seq(scala32, mainScala)

val zioVersion = "2.0.2"
val pravegaVersion = "0.11.0"
val zioConfigVersion = "2.0.4"

inThisBuild(
  List(
    organization := "dev.cheleb",
    homepage := Some(url("https://github.com/cheleb/zio-pravega")),
    licenses := List(
      "Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")
    ),
    useCoursier := false,
    scalaVersion := mainScala,
    crossScalaVersions := allScala,
    Test / parallelExecution := false,
    Test / fork := true,
    run / fork := true,
    sonatypeCredentialHost := "s01.oss.sonatype.org",
    sonatypeRepository := "https://s01.oss.sonatype.org/service/local",
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
    scalafixScalaBinaryVersion := "2.13"
  )
)

val zioConfig =
  Seq("zio-config", "zio-config-magnolia", "zio-config-typesafe").map(d =>
    "dev.zio" %% d % zioConfigVersion
  )

lazy val pravega =
  project
    .in(file("."))
    .enablePlugins(BuildInfoPlugin)
    .settings(
      name := "zio-pravega",
      scalafmtOnCompile := true,
      fork := true
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
      libraryDependencies ++= Seq(
        "dev.zio" %% "zio-streams" % zioVersion,
        "dev.zio" %% "zio-test" % zioVersion % Test,
        "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
        "org.scalatest" %% "scalatest" % "3.2.13" % Test,
        "io.pravega" % "pravega-client" % pravegaVersion,
        "org.testcontainers" % "testcontainers" % "1.17.2" % Test,
        "dev.zio" %% "zio-zmx" % "2.0.0-RC4" % Test,
        "org.scala-lang.modules" %% "scala-collection-compat" % "2.8.1",
        "ch.qos.logback" % "logback-classic" % "1.2.11" % Test,
        "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % Test,
        "io.envoyproxy.protoc-gen-validate" % "pgv-java-stub" % "0.6.7" % Test,
        "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
      ) ++ zioConfig,
      testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
    )
    .settings(
      libraryDependencies ++= Seq(
        "com.thesamet.scalapb.common-protos" %% "proto-google-common-protos-scalapb_0.11" % "2.5.0-3" % "protobuf",
        "com.thesamet.scalapb.common-protos" %% "proto-google-common-protos-scalapb_0.11" % "2.5.0-3" % Test,
        "com.thesamet.scalapb.common-protos" %% "pgv-proto-scalapb_0.11" % "0.6.3-0" % "protobuf",
        "com.thesamet.scalapb.common-protos" %% "pgv-proto-scalapb_0.11" % "0.6.3-0" % Test
      ),
      Test / PB.targets := Seq(
        scalapb.gen() -> (Test / sourceManaged).value / "scalapb"
      )
    )

lazy val docs = project // new documentation project
  .in(file("zio-pravega-docs")) // important: it must not be docs/
  .dependsOn(pravega)
  .settings(
    publish / skip := true,
    moduleName := "zio-pravega-docs",
    ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(pravega),
    ScalaUnidoc / unidoc / target := (LocalRootProject / baseDirectory).value / "website" / "static" / "api",
    cleanFiles += (ScalaUnidoc / unidoc / target).value,
    docusaurusCreateSite := docusaurusCreateSite
      .dependsOn(Compile / unidoc)
      .value,
    docusaurusPublishGhpages := docusaurusPublishGhpages
      .dependsOn(Compile / unidoc)
      .value,
    mdocVariables := Map(
      "VERSION" -> "0.2.0",
      "ORG" -> organization.value
    )
  )
  .enablePlugins(MdocPlugin, DocusaurusPlugin, ScalaUnidocPlugin)

addCommandAlias("fmt", "all scalafmtSbt scalafmt test / scalafmt")
addCommandAlias(
  "check",
  "all scalafmtSbtCheck scalafmtCheck test / scalafmtCheck"
)
