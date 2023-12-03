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
