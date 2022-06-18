---
sidebar_position: 3
---
# Read from stream

```scala mdoc
import zio._
import zio.pravega._

import io.pravega.client.stream.impl.UTF8StringSerializer

object StreamReadExample extends ZIOAppDefault {

  val clientConfig = PravegaClientConfig.default

  val stringReaderSettings =
    ReaderSettingsBuilder()
      .withSerializer(new UTF8StringSerializer)

  private val program = for {
    _ <- PravegaAdmin.readerGroup(
      "a-scope",
      "a-reader-group",
      "a-stream"
    )
    stream <- PravegaStream.stream(
      "a-reader-group",
      stringReaderSettings
    )
    _ <- stream
      .tap(m => ZIO.debug(m.toString()))
      .take(10)
      .runFold(0)((s, _) => s + 1)

  } yield ()

  override def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
    program.provide(
      Scope.default,
      PravegaAdmin.live(clientConfig),
      PravegaStream.fromScope("a-scope", clientConfig)
    )

}

```
