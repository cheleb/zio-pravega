package zio.pravega.docs

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

  override def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
    ZIO
      .scoped {
        for {
          sink <- PravegaStreamService.sink(
            "a-stream",
            stringWriterSettings
          )
          _ <- testStream(1, 10)
            .tap(p => ZIO.debug(p.toString()))
            .run(sink)
          _ <- ZIO.log("Coucou")

        } yield ()
      }
      .provide(
        Scope.default,
        ZLayer(ZIO.succeed(PravegaClientConfigBuilder().build())),
        PravegaStreamLayer.fromScope("a-scope")
      )

}
