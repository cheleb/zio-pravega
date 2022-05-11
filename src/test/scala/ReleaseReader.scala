import zio._
import zio.Console._

import io.pravega.client.ClientConfig

import zio.pravega.PravegaAdminLayer
import zio.pravega.PravegaAdminService

object ReleaseReader extends ZIOAppDefault {

  val program = for {
    n <- PravegaAdminService.readerOffline("zio-scope", "coco1")
    _ <- printLine(s"Offined $n reader(s).")
  } yield ()

  override def run: URIO[Any, ExitCode] =
    program
      .provide(
        Scope.default,
        ZLayer.succeed(ClientConfig.builder().build()),
        PravegaAdminLayer.layer
      )
      .exitCode

}
