import zio._
import zio.Console._
import zio.pravega.PravegaAdmin
import zio.pravega.PravegaAdminService
import io.pravega.client.ClientConfig

object ReleaseReader extends ZIOAppDefault {

  val program = for {
    n <- PravegaAdminService(_.readerOffline("zio-scope", "coco1"))
    _ <- printLine(s"Offined $n reader(s).")
  } yield ()

  override def run: ZIO[Environment with ZEnv with ZIOAppArgs, Any, Any] =
    program
      .provideCustom(PravegaAdmin.layer(ClientConfig.builder().build()))
      .exitCode

}
