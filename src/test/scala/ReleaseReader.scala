import zio._
import zio.Console._

import io.pravega.client.ClientConfig

import zio.pravega.PravegaAdmin

object ReleaseReader extends ZIOAppDefault {

  val clientConfig = ClientConfig.builder().build()

  val program = for {
    n <- PravegaAdmin.readerOffline("a-scope", "a-reader-group")
    _ <- printLine(s"Offined $n reader(s).")
  } yield ()

  override def run: URIO[Any, ExitCode] = program.provide(Scope.default, PravegaAdmin.live(clientConfig)).exitCode

}
