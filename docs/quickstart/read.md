---
sidebar_position: 3
---
# Read from stream

```scala mdoc
import zio._
import zio.pravega._
import zio.pravega.admin._

import io.pravega.client.stream.impl.UTF8StringSerializer

object StreamReadExample extends ZIOAppDefault {

  val clientConfig = PravegaClientConfig.default

  val stringReaderSettings =
    ReaderSettingsBuilder()
      .withSerializer(new UTF8StringSerializer)

  private val program = for {
    _ <- PravegaReaderGroupManager.createReaderGroup(
      "a-reader-group",
      "a-stream"
    )
    stream = PravegaStream.stream(
      "a-reader-group",
      stringReaderSettings
    )
    _ <- stream
      .tap(m => ZIO.debug(m.toString()))
      .take(10)
      .runFold(0)((s, _) => s + 1)

  } yield ()

  override def run: ZIO[Any, Throwable, Unit] =
    program.provide(
      Scope.default,
      PravegaClientConfig.live,
      PravegaReaderGroupManager.live("a-scope"),
      PravegaStream.fromScope("a-scope")
    )

}

```