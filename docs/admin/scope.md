---
sidebar_position: 1
---
# Scopes
Scopes are namespaces for [Streams](stream.md), they must be created before being referenced (no automatic creation).


```scala mdoc:silent
import zio._
import zio.pravega.admin._

def initScope(scope: String): ZIO[PravegaStreamManager,Throwable,Unit] =
    for {
      scopeCreated <- PravegaStreamManager.createScope(scope)
      _ <- ZIO.when(scopeCreated)(Console.printLine(s"Scope $scope just created"))
    } yield ()

```
