// scalafmt: { maxColumn = 120, style = defaultWithAlign }

addSbtPlugin("dev.cheleb"                      % "sbt-plantuml"     % "0.1.1")
addSbtPlugin("com.dwijnand"                    % "sbt-dynver"       % "4.1.1")
addSbtPlugin("org.scalameta"                   % "sbt-scalafmt"     % "2.5.3")
addSbtPlugin("ch.epfl.scala"                   % "sbt-scalafix"     % "0.13.0")
addSbtPlugin("com.eed3si9n"                    % "sbt-buildinfo"    % "0.13.1")
addSbtPlugin("com.github.sbt"                  % "sbt-ci-release"   % "1.8.0")
addSbtPlugin("com.thesamet"                    % "sbt-protoc"       % "1.0.7")
addSbtPlugin("org.scoverage"                   % "sbt-scoverage"    % "2.3.0")
addSbtPlugin("com.github.sbt"                  % "sbt-unidoc"       % "0.5.0")
addSbtPlugin("org.scalameta"                   % "sbt-mdoc"         % "2.6.2")
addSbtPlugin("org.wartremover"                 % "sbt-wartremover"  % "3.2.5")
addSbtPlugin("com.github.sbt"                  % "sbt-unidoc"       % "0.5.0")
addSbtPlugin("com.github.sbt"                  % "sbt-ghpages"      % "0.8.0")
addSbtPlugin("com.github.sbt"                  % "sbt-site-paradox" % "1.7.0")
addSbtPlugin("com.github.sbt"                  % "sbt-git"          % "2.1.0")
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin"   % "0.11.17"
