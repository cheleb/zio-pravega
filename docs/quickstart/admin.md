---
sidebar_position: 1
---

# Scope and stream

## Description
First of all we need to create a scope (aka namespace) and a [stream](https://cncf.pravega.io/docs/nightly/pravega-concepts/#streams).

Streams are the main data unit in Pravega. They are able to dynamicly scale up and down.

Scaling policies are explained in details in the [auto-scaling Pravega documentation](https://cncf.pravega.io/docs/nightly/pravega-concepts/#elastic-streams-auto-scaling).


The number of shards determines the number of readers that are reading from the stream in parallel.

In short, streams are created with:
* a fixed number of shards.
* a variable number of shards (Data-based or Event-based).

When varying, shards are splitted / merged in relation of the load of the system. This load is determined as:
* Event-based: the number of events written to the stream.
* Data-based: the quantity of bytes written to the stream.

Variable number of shards is useful when you want to scale up or down the number of readers/writers dynamically.


## Show me the code

```scala mdoc
import zio._
import zio.pravega._
import zio.pravega.admin._

import io.pravega.client.stream.StreamConfiguration
import io.pravega.client.stream.ScalingPolicy

object CreateResourcesExample extends ZIOAppDefault {
  
  val clientConfig = PravegaClientConfig.default

  private val streamConfiguration = StreamConfiguration.builder
    .scalingPolicy(ScalingPolicy.fixed(8))
    .build

  private val program = for {
    _ <- PravegaStreamManager.createScope("a-scope")
    _ <- PravegaStreamManager.createStream(
      "a-scope",
      "a-stream",
      streamConfiguration
    )
  } yield ()

  override def run: ZIO[Any, Throwable, Unit] =
    program
      .provide(
        Scope.default,
        PravegaClientConfig.live,
        PravegaStreamManager.live
      )

}
```

