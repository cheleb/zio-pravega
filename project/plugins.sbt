// scalafmt: { maxColumn = 120, style = defaultWithAlign }

addSbtPlugin("dev.cheleb"     % "sbt-plantuml"   % "0.0.10")
addSbtPlugin("com.dwijnand"   % "sbt-dynver"     % "4.1.1")
addSbtPlugin("org.scalameta"  % "sbt-scalafmt"   % "2.5.2")
addSbtPlugin("ch.epfl.scala"  % "sbt-scalafix"   % "0.11.0")
addSbtPlugin("com.eed3si9n"   % "sbt-buildinfo"  % "0.11.0")
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.5.12")
addSbtPlugin("com.thesamet"   % "sbt-protoc"     % "1.0.6")
addSbtPlugin("org.scoverage"  % "sbt-scoverage"  % "2.0.9")
addSbtPlugin("com.github.sbt" % "sbt-unidoc"     % "0.5.0")
addSbtPlugin("org.scalameta"  % "sbt-mdoc"       % "2.4.0")
addSbtPlugin("org.wartremover" % "sbt-wartremover" % "3.1.5")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.14"
