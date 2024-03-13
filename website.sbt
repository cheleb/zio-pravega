addCommandAlias("website", "docs/mdoc; makeSite")

lazy val currentYear: String =
  java.util.Calendar.getInstance().get(java.util.Calendar.YEAR).toString

enablePlugins(
  SiteScaladocPlugin,
  SitePreviewPlugin,
  ScalaUnidocPlugin,
  GhpagesPlugin
)

ScalaUnidoc / siteSubdirName := ""
addMappingsToSiteDir(
  ScalaUnidoc / packageDoc / mappings,
  ScalaUnidoc / siteSubdirName
)
git.remoteRepo  := "git@github.com:cheleb/zio-pravega.git"
ghpagesNoJekyll := true

Compile / doc / scalacOptions ++= (scalaVersion.value match {
  case thisScalaVersion if thisScalaVersion.startsWith("3.") =>
    Seq(
      "-siteroot",
      "zio-pravega-docs/target/mdoc",
      "-groups",
      "-external-urls scala=https://cheleb.github.io/zio-pravega/",
      "-project-version",
      version.value,
      "-revision",
      version.value,
      "-project-footer",
      s"Copyright (c) 2022-$currentYear, Olivier NOUGUIER",
      "-social-links:github::https://github.com/cheleb,twitter::https://twitter.com/oNouguier",
      "-Ygenerate-inkuire",
      "-skip-by-regex:zio\\.pravega\\.docs\\..*",
      "-skip-by-regex:html\\..*"
    )
  case _ => Seq.empty
})
