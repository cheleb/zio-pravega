// scalafmt: { maxColumn = 120, style = defaultWithAlign }

addSbtPlugin("dev.cheleb"     % "sbt-plantuml"   % "0.0.5")
addSbtPlugin("com.dwijnand"   % "sbt-dynver"     % "4.1.1")
addSbtPlugin("org.scalameta"  % "sbt-scalafmt"   % "2.5.0")
addSbtPlugin("ch.epfl.scala"  % "sbt-scalafix"   % "0.10.4")
addSbtPlugin("com.eed3si9n"   % "sbt-buildinfo"  % "0.11.0")
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.5.11")
addSbtPlugin("com.thesamet"   % "sbt-protoc"     % "1.0.6")
addSbtPlugin("org.scoverage"  % "sbt-scoverage"  % "2.0.7")
addSbtPlugin("com.github.sbt" % "sbt-unidoc"     % "0.5.0")
addSbtPlugin("org.scalameta"  % "sbt-mdoc"       % "2.3.7")
//addSbtPlugin("io.github.davidgregory084" % "sbt-tpolecat"   % "0.4.1")
addSbtPlugin("org.wartremover" % "sbt-wartremover" % "3.0.12")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.13"
