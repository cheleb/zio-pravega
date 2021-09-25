import java.net.URI
import zio._
import zio.Console._

object ReleaseReader extends ZIOAppDefault {

  val program = for {
    n <- PravegaAdmin
          .readerOffline("coco1")
          .provideCustomLayer(
            PravegaAdmin
              .readerGroupManager("zio-scope", new URI("tcp://localhost:9090"))
              .toLayer
          )
    _ <- printLine(s"Offined $n reader(s).")
  } yield ()

  override def run: ZIO[Environment with ZEnv with Has[ZIOAppArgs], Any, Any] = program.exitCode

}
