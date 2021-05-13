import java.net.URI
import zio._
import zio.Console._

object ReleaseReader extends App {

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

  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    program.exitCode

}
