import zio._
import zio.Console._

import zio.pravega.PravegaAdminService
import io.pravega.client.ClientConfig
import zio.pravega.PravegaAdmin

object ReleaseReader extends ZIOAppDefault {

  val program = for {
    n <- PravegaAdminService(_.readerOffline("zio-scope", "coco1"))
    _ <- printLine(s"Offined $n reader(s).")
  } yield ()

  override def run: URIO[Any, ExitCode] =
    program
      .provide(
        Scope.default,
        ZLayer.succeed(ClientConfig.builder().build()),
        PravegaAdmin.layer
      )
      .exitCode

}
