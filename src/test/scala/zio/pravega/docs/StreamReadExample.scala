package zio.pravega.docs

import zio._
import zio.pravega._

import zio.pravega.serder.*
import zio.pravega.admin.PravegaReaderGroupManager

object StreamReadExample extends ZIOAppDefault {

  val stringReaderSettings: ReaderSettings[String] =
    ReaderSettingsBuilder().withDeserializer(UTF8StringScalaDeserializer)

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
