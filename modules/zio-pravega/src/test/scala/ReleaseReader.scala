import zio._

import io.pravega.client.ClientConfig

import zio.pravega.admin.PravegaReaderGroupManager

object ReleaseReader extends ZIOAppDefault {

  private val clientConfig = ClientConfig.builder().build()

  private val program = for {
    n <- PravegaReaderGroupManager.readerOffline("a-reader-group")
    _ <- ZIO.debug(f"Offined $n%d reader(s).")
  } yield ()

  override def run = program
    .provide(Scope.default, ZLayer.succeed(clientConfig), PravegaReaderGroupManager.live("a-scope"))

}
