## Scopes
Scopes are namespaces for [Streams](stream.md), they must be created before being referenced (no automatic creation).


```scala mdoc:silent
import io.pravega.client.ClientConfig

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
