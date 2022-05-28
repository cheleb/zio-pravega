package zio.pravega.docs

import zio._
import zio.pravega._

import io.pravega.client.stream.impl.UTF8StringSerializer

object StreamReadExample extends ZIOAppDefault {

  val stringReaderSettings =
    ReaderSettingsBuilder()
      .withSerializer(new UTF8StringSerializer)

  override def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
    ZIO
      .scoped {
        for {
          _ <- PravegaAdminService.readerGroup(
            "a-scope",
            "a-reader-group",
            "a-stream"
          )
          stream <- PravegaStreamService.stream(
            "a-reader-group",
            stringReaderSettings
          )
          _ <- stream
            .tap(m => ZIO.debug(m.toString()))
            .take(10)
            .runFold(0)((s, _) => s + 1)

        } yield ()
      }
      .provide(
        Scope.default,
        ZLayer(ZIO.succeed(PravegaClientConfigBuilder().build())),
        PravegaAdminLayer.layer,
        PravegaStreamLayer.fromScope("a-scope")
      )

}
