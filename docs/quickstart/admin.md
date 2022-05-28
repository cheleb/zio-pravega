---
sidebar_position: 1
---

# Create scope and stream

```scala mdoc
import zio._
import zio.pravega._

import io.pravega.client.stream.StreamConfiguration
import io.pravega.client.stream.ScalingPolicy

object CreateResourcesExample extends ZIOAppDefault {

  private val streamConfiguration = StreamConfiguration.builder
    .scalingPolicy(ScalingPolicy.fixed(8))
    .build

  override def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
    ZIO
      .scoped {
        for {
          _ <- PravegaAdminService.createScope("a-scope")
          _ <- PravegaAdminService.createStream(
            "a-scope",
            "a-stream",
            streamConfiguration
          )
        } yield ()
      }
      .provide(
        ZLayer(ZIO.succeed(PravegaClientConfigBuilder().build())),
        PravegaAdminLayer.layer
      )

}
```

