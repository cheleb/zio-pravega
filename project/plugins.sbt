// scalafmt: { maxColumn = 120, style = defaultWithAlign }
addSbtPlugin("com.thesamet"              % "sbt-protoc"           % "1.0.6")
addSbtPlugin("org.scoverage"             % "sbt-scoverage"        % "1.9.3")
addSbtPlugin("com.github.sbt"            % "sbt-unidoc"           % "0.5.0")
addSbtPlugin("org.scalameta"             % "sbt-mdoc"             % "2.3.0")
addSbtPlugin("org.scalameta"             % "sbt-scalafmt"         % "2.4.6")
addSbtPlugin("ch.epfl.scala"             % "sbt-scalafix"         % "0.9.34")
addSbtPlugin("io.github.davidgregory084" % "sbt-tpolecat"         % "0.1.20")
addSbtPlugin("com.eed3si9n"              % "sbt-buildinfo"        % "0.10.0")
//addSbtPlugin("com.geirsson"              % "sbt-ci-release"       % "1.5.7")
addSbtPlugin("com.codecommit"            % "sbt-github-packages"  % "0.5.3")
addSbtPlugin("com.dwijnand"              % "sbt-dynver"           % "4.1.1") 
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.11"
