---
sidebar_position: 1
---
# Stream

Central Pravega abstraction, must be explictly created, in a [Scope](scope.md).

```scala mdoc:invisible
import zio._
import zio.Console._
import zio.pravega._
import io.pravega.client.stream.StreamConfiguration
import io.pravega.client.stream.ScalingPolicy
```


```scala mdoc
def initStream(streamName: String, scope: String)
: ZIO[Scope & Console & PravegaAdminService,Throwable,Unit] =
    for {
      streamCreated <- PravegaAdmin(
        _.createStream(
          streamName,
          StreamConfiguration.builder
            .scalingPolicy(ScalingPolicy.fixed(8))
            .build,
          scope
        )
      )
      _ <- ZIO.when(streamCreated)(
        printLine(s"Stream $streamName just created")
      )
    } yield ()

````

