import zio._
import zio.Console._

import io.pravega.client.ClientConfig
import zio.pravega.PravegaAdmin
import zio.pravega.PravegaAdminLayer

object ReleaseReader extends ZIOAppDefault {

  val program = for {
    n <- PravegaAdmin(_.readerOffline("zio-scope", "coco1"))
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
