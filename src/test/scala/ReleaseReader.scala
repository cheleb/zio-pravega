import zio._
import zio.Console._

import io.pravega.client.ClientConfig

import zio.pravega.admin.PravegaReaderGroupManager

object ReleaseReader extends ZIOAppDefault {

  val clientConfig = ClientConfig.builder().build()

  val program = for {
    n <- PravegaReaderGroupManager.readerOffline("a-reader-group")
    _ <- printLine(s"Offined $n reader(s).")
  } yield ()

  override def run = program
    .provide(Scope.default, ZLayer.succeed(clientConfig), PravegaReaderGroupManager.live("a-scope"))

}
