---
sidebar_position: 1
---
# Scopes
Scopes are namespaces for [Streams](stream.md), they must be created before being referenced (no automatic creation).


```scala mdoc:invisible
import zio._
import zio.Console._
import zio.pravega._
```

```scala mdoc

def initScope(scope: String): ZIO[PravegaAdminService with Console,Throwable,Unit] =
    for {
      scopeCreated <- PravegaAdminService(_.createScope(scope))
      _ <- ZIO.when(scopeCreated)(printLine(s"Scope $scope just created"))
    } yield ()

```
