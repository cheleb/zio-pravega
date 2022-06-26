---
sidebar_position: 1
---
# Scopes
Scopes are namespaces for [Streams](stream.md), they must be created before being referenced (no automatic creation).


```scala mdoc:silent
import zio._
import zio.Console._
import zio.pravega._

def initScope(scope: String): ZIO[Scope & PravegaAdminService & Console,Throwable,Unit] =
    for {
      scopeCreated <- PravegaAdmin.createScope(scope)
      _ <- ZIO.when(scopeCreated)(printLine(s"Scope $scope just created"))
    } yield ()

```
