---
sidebar_position: 2
---

# Write to stream

```scala mdoc
import zio._
import zio.pravega._
import zio.stream._

import io.pravega.client.stream.impl.UTF8StringSerializer

object StreamWriteExample extends ZIOAppDefault {

  val stringWriterSettings =
    WriterSettingsBuilder()
      .eventWriterConfigBuilder(_.enableLargeEvents(true))
      .withSerializer(new UTF8StringSerializer)

  private def testStream(a: Int, b: Int): ZStream[Any, Nothing, String] =
    ZStream
      .fromIterable(a to b)
      .map(i => f"$i%04d_name $i")

  val program = for {
    _ <- ZIO.debug("Creating stream")
    sink = PravegaStream.sink(
      "a-stream",
      stringWriterSettings
    )
    _ <- testStream(1, 10)
      .tap(p => ZIO.debug(p.toString()))
      .run(sink)
    _ <- ZIO.log("Coucou")

  } yield ()

  override def run: ZIO[Any, Throwable, Unit] =
    program.provide(
      Scope.default,
      PravegaClientConfig.live,
      PravegaStream.fromScope("a-scope")
    )

}

```
