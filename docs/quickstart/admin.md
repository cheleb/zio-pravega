---
sidebar_position: 1
---

# Create scope and stream

First of all we need to create a scope (aka namespace) and a [stream].

```scala mdoc
import zio._
import zio.pravega._

import io.pravega.client.stream.StreamConfiguration
import io.pravega.client.stream.ScalingPolicy

object CreateResourcesExample extends ZIOAppDefault {
  
  val clientConfig = PravegaClientConfig.default

  private val streamConfiguration = StreamConfiguration.builder
    .scalingPolicy(ScalingPolicy.fixed(8))
    .build

  private val program = for {
    _ <- PravegaAdmin.createScope("a-scope")
    _ <- PravegaAdmin.createStream(
      "a-scope",
      "a-stream",
      streamConfiguration
    )
  } yield ()

  override def run: ZIO[Scope, Throwable, Unit] =
    program
      .provideSome(
        PravegaAdmin.live(clientConfig)
      )

}
```

