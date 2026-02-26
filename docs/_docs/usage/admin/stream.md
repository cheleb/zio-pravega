# Stream

```scala mdoc:invisible
import io.pravega.client.ClientConfig
import io.pravega.client.stream.ScalingPolicy
import io.pravega.client.stream.StreamConfiguration
import zio.*
import zio.pravega.admin.*
import scala.jdk.CollectionConverters.*

```

Central Pravega abstraction, [Stream](https://cncf.pravega.io/docs/nightly/pravega-concepts/#streams) must be explictly created, in a [Scope](https://cncf.pravega.io/docs/nightly/pravega-concepts/#scopes).

In the example below we will create a "sales" scope and a stream "events" in this scope.

## Create a stream

Streams are created in a scope, and must be explicitly created.

```scala mdoc:silent

val streamConfiguration = StreamConfiguration.builder
    .scalingPolicy(ScalingPolicy.fixed(3))
    .build

val aStream:  RIO[PravegaStreamManager, Boolean] =
    PravegaStreamManager.createStream("sales", "events", streamConfiguration)
```

When creating a stream, we need to provide a `StreamConfiguration` that defines the stream properties, in this case we are using a fixed scaling policy with 3 segments.

Scaling is one of the most important features of Pravega, it allows to dynamically adapt the stream to the load, by adding or removing segments.

Scaling is a complex topic, and we will cover it in a dedicated section.

Simply said a `ScalingPolicy` can be:

* fixed: the number of segments is fixed, and the segments are evenly distributed across the nodes.
* by rate: the number of segments is dynamically adjusted to the rate of events.
* by size: the number of segments is dynamically adjusted to the size of the stream.

The `aStream` is a `RIO[PravegaStreamManager,Boolean]` that:

* will produce `true` if the stream was created or `false` if it already existed.
* depends on the `PravegaStreamManager` capability.

As before, we need to provide the capability, which is the role of `ZLayer`.

## Reader groups

A [Reader Group](https://cncf.pravega.io/docs/nightly/pravega-concepts/#writers-readers-reader-groups) is a named collection of Readers, which together perform parallel reads from a given Stream.

It must created expliciyly.

```scala mdoc:silent
  val readerGroup: RIO[PravegaReaderGroupManager, Boolean] =
    PravegaReaderGroupManager.createReaderGroup(
      "stats-application",
      "sales", "cancellation"
    )
```

The `readerGroup` is a `RIO[PravegaReaderGroupManager,Boolean]` that:

* will produce `true` if the reader group was created or `false` if it already existed.
* depends on the `PravegaReaderGroupManager` capability.

As before, we need to provide the capability, which is the role of `ZLayer`:

```scala mdoc:silent
val readerGroupManager: RLayer[Scope & ClientConfig, PravegaReaderGroupManager] =
    PravegaReaderGroupManager.live("sales")
```

Note that the `PravegaReaderGroupManager` capability depends on the `Scope` and `ClientConfig` capability, which are provided by another layer.
Also that `PravegaReaderGroupManager` is parameterized by the scope name, which is "sales" in this case.

Stream manager allows to create, delete, list, seal, truncate streams.

## Sealing a stream

Before deleting, truncatig a stream, it must be sealed.

```scala mdoc:silent
val sealStream: RIO[PravegaStreamManager, Boolean] =
    PravegaStreamManager
      .sealStream("sales", "events")
```

## Truncation

A [Truncation](https://pravega.io/docs/snapshot/retention/#retention-service) is a mechanism to remove data from a Stream.

```scala mdoc:silent
  for {
          readerGroup <- PravegaReaderGroupManager.openReaderGroup("g1")
          streamCuts   = readerGroup.getStreamCuts()
          _ <- ZIO.foreach(streamCuts.asScala.toList) { case (stream, streamCut) =>
                 ZIO.logDebug(s"Stream: ${stream.getStreamName}, StreamCut: $streamCut") *>
                   PravegaStreamManager.truncateStream("sales", stream.getStreamName(), streamCut)
               }
  } yield ()
```
