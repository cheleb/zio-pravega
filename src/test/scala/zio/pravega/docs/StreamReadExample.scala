package zio.pravega.docs

import zio._
import zio.pravega._

import io.pravega.client.stream.impl.UTF8StringSerializer
import zio.pravega.admin.PravegaReaderGroupManager

object StreamReadExample extends ZIOAppDefault {

  val stringReaderSettings: ReaderSettings[String] = ReaderSettingsBuilder().withSerializer(new UTF8StringSerializer)

  private val program = for {
    _     <- PravegaReaderGroupManager.createReaderGroup("a-reader-group", "a-stream")
    stream = PravegaStream.stream("a-reader-group", stringReaderSettings)
    count <- stream.tap(m => ZIO.debug(m)).take(10).runCount
    _     <- Console.printLine(f"Read $count%d elements.")
  } yield ()

  override def run: ZIO[Scope, Throwable, Unit] = program.provideSome[Scope](
    PravegaClientConfig.live,
    PravegaReaderGroupManager.live("a-scope"),
    PravegaStream.fromScope("a-scope")
  )

}
