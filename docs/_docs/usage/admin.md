# Creating Pravega resources

Pravega resources must be created before being used, this is the role of the ZIO Pravega admin API.

In the example below we will create a "sales" scope and a stream "events" in this scope.




## Scopes
Scopes are namespaces for [Streams](streaming.md), they must be created before being referenced (no automatic creation).


```scala mdoc:silent
import io.pravega.client.ClientConfig
import io.pravega.client.stream.ScalingPolicy
import io.pravega.client.stream.StreamConfiguration

import zio._
import zio.pravega._
import zio.pravega.admin._


val aScopeUIO: RIO[PravegaStreamManager,Boolean] =
   PravegaStreamManager.createScope("sales")
```

The `aScopeUIO` is a `RIO[PravegaStreamManager,Boolean]` that:
* will produce `true` if the scope was created or `false` if it already existed.
* depends on the `PravegaStreamManager` capability.

This is a common pattern in `ZIO` applications, where the capability is provided by the environment and the result is a `ZIO` value that can be composed with other `ZIO` values.

To provide the capability we need to create a `PravegaStreamManager` to this effect, which is the role of `ZLayer`:

```scala mdoc:silent
val manager: RLayer[Scope & ClientConfig, PravegaStreamManager] =
                           PravegaStreamManager.live
```

This `ZLayer` in turn depends on `Scope` and `ClientConfig` capabilities, which are provided by another layers, and `ZIO` this will combile all these layers:

```scala mdoc:silent
val createScope: Task[Boolean] =
    aScopeUIO
      .provide(
        Scope.default,
        PravegaClientConfig.live,
        PravegaStreamManager.live
      )
```

By producing a `Task[Unit]` value, then a runnable IO.


## Streams

Central Pravega abstraction, [Stream](https://cncf.pravega.io/docs/nightly/pravega-concepts/#streams) must be explictly created, in a [Scope](https://cncf.pravega.io/docs/nightly/pravega-concepts/#scopes).


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
