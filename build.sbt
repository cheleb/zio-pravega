val scala213  = "2.13.10"
val scala32   = "3.2.2"
val mainScala = scala213
val allScala  = Seq(scala32, scala213)

val zioVersion     = "2.0.10"
val pravegaVersion = "0.12.0"

inThisBuild(
  List(
    organization := "dev.cheleb",
    homepage     := Some(url("https://github.com/cheleb/zio-pravega")),
    licenses := List(
      "Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")
    ),
    useCoursier              := false,
    scalaVersion             := mainScala,
    crossScalaVersions       := allScala,
    Test / parallelExecution := false,
    Test / fork              := true,
    run / fork               := true,
    sonatypeCredentialHost   := "s01.oss.sonatype.org",
    sonatypeRepository       := "https://s01.oss.sonatype.org/service/local",
    pgpPublicRing            := file("/tmp/public.asc"),
    pgpSecretRing            := file("/tmp/secret.asc"),
    pgpPassphrase            := sys.env.get("PGP_PASSWORD").map(_.toArray),
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
    semanticdbEnabled          := true,
    semanticdbVersion          := scalafixSemanticdb.revision, // only required for Scala 2.x
    scalafixScalaBinaryVersion := "2.13",
//    scalafixOnCompile          := true,
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

lazy val pravega =
  project
    .in(file("."))
    .enablePlugins(BuildInfoPlugin)
    .settings(
      name              := "zio-pravega",
      scalafmtOnCompile := true,
      fork              := true
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
        "com.typesafe"                      % "config"                   % "1.4.2",
        "dev.zio"                          %% "zio-streams"              % zioVersion,
        "dev.zio"                          %% "zio-test"                 % zioVersion                              % Test,
        "dev.zio"                          %% "zio-test-sbt"             % zioVersion                              % Test,
        "dev.zio"                          %% "zio-logging-slf4j-bridge" % "2.1.10"                                 % Test,
        "org.scalatest"                    %% "scalatest"                % "3.2.15"                                % Test,
        "io.pravega"                        % "pravega-client"           % pravegaVersion,
        "org.testcontainers"                % "testcontainers"           % "1.17.6"                                % Test,
        "org.scala-lang.modules"           %% "scala-collection-compat"  % "2.9.0",
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
        scalapb.gen() -> (Test / sourceManaged).value / "scalapb"
      )
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
    docusaurusCreateSite := docusaurusCreateSite
      .dependsOn(Compile / unidoc)
      .value,
    docusaurusPublishGhpages := docusaurusPublishGhpages
      .dependsOn(Compile / unidoc)
      .value,
    mdocVariables := Map(
      "VERSION" -> version.value,
      "ORG"     -> organization.value
    )
  )
  .disablePlugins(WartRemover)
  .enablePlugins(MdocPlugin, DocusaurusPlugin, ScalaUnidocPlugin, PlantUMLPlugin)
  .settings(
    plantUMLSource           := (pravega / baseDirectory).value / "docs",
    Compile / plantUMLTarget := "mdoc"
  )

addCommandAlias("fmt", "all scalafmtSbt scalafmt test / scalafmt")
addCommandAlias(
  "check",
  "all scalafmtSbtCheck scalafmtCheck test / scalafmtCheck"
)
