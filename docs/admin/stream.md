---
sidebar_position: 1
---
# Stream

Central Pravega abstraction, [Stream](https://cncf.pravega.io/docs/nightly/pravega-concepts/#streams) must be explictly created, in a [Scope](scope.md).

```scala mdoc:silent
import zio._
import zio.pravega._
import zio.pravega.admin._
import io.pravega.client.stream.StreamConfiguration
import io.pravega.client.stream.ScalingPolicy

def initStream(streamName: String, scope: String)
: ZIO[PravegaStreamManager,Throwable,Unit] =
    for {
      streamCreated <- PravegaStreamManager.createStream(
          scope,
          streamName,
          StreamConfiguration.builder
            .scalingPolicy(ScalingPolicy.fixed(8))
            .build
        )      
      _ <- ZIO.when(streamCreated)(
        Console.printLine(s"Stream $streamName just created")
      )
    } yield ()

```

# Reader group

A [Reader Group](https://cncf.pravega.io/docs/nightly/pravega-concepts/#writers-readers-reader-groups) is a named collection of Readers, which together perform parallel reads from a given Stream

It must created expliciyly 

```scala mdoc:silent
  PravegaReaderGroupManager.createReaderGroup(
              "a-group-name",
              "stream-a", "stream-b"
            )
```