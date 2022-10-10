package zio.pravega.docs

import zio._
import zio.pravega._

import io.pravega.client.stream.impl.UTF8StringSerializer

object StreamReadExample extends ZIOAppDefault {

  val stringReaderSettings = ReaderSettingsBuilder().withSerializer(new UTF8StringSerializer)

  private val program = for {
    _     <- PravegaAdmin.createReaderGroup("a-scope", "a-reader-group", "a-stream")
    stream = PravegaStream.stream("a-reader-group", stringReaderSettings)
    count <- stream.tap(m => ZIO.debug(m.toString())).take(10).runCount
    _     <- Console.printLine(s"Read $count elements.")
  } yield ()

  override def run: ZIO[Any, Throwable, Unit] = program.provide(
    Scope.default,
    PravegaAdmin.live(PravegaClientConfig.default),
    PravegaStream.fromScope("a-scope", PravegaClientConfig.default)
  )

}
