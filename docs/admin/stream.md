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


```scala mdoc:silent
def initStream(streamName: String, scope: String)
: ZIO[Scope & Console & PravegaAdminService,Throwable,Unit] =
    for {
      streamCreated <- PravegaAdminService.createStream(
          scope,
          streamName,
          StreamConfiguration.builder
            .scalingPolicy(ScalingPolicy.fixed(8))
            .build
        )
      
      _ <- ZIO.when(streamCreated)(
        printLine(s"Stream $streamName just created")
      )
    } yield ()

```

# Reader group

A [Reader Group](https://cncf.pravega.io/docs/nightly/pravega-concepts/#writers-readers-reader-groups) is a named collection of Readers, which together perform parallel reads from a given Stream

It must created expliciyly 

```scala mdoc:silent
  PravegaAdminService.readerGroup(
              "a-scope",
              "a-group-name",
              "stream-a", "stream-b"
            )
```