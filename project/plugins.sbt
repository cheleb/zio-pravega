// scalafmt: { maxColumn = 120, style = defaultWithAlign }
addSbtPlugin("com.dwijnand"              % "sbt-dynver"     % "4.1.1")
addSbtPlugin("org.scalameta"             % "sbt-scalafmt"   % "2.4.6")
addSbtPlugin("ch.epfl.scala"             % "sbt-scalafix"   % "0.10.3")
addSbtPlugin("com.eed3si9n"              % "sbt-buildinfo"  % "0.11.0")
addSbtPlugin("com.github.sbt"            % "sbt-ci-release" % "1.5.10")
addSbtPlugin("com.thesamet"              % "sbt-protoc"     % "1.0.6")
addSbtPlugin("org.scoverage"             % "sbt-scoverage"  % "2.0.6")
addSbtPlugin("com.github.sbt"            % "sbt-unidoc"     % "0.5.0")
addSbtPlugin("org.scalameta"             % "sbt-mdoc"       % "2.3.6")
addSbtPlugin("io.github.davidgregory084" % "sbt-tpolecat"   % "0.4.1")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.12"
