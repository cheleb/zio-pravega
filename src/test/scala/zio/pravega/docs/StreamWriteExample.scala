package zio.pravega.docs

import zio._
import zio.pravega._
import zio.stream._

import io.pravega.client.stream.impl.UTF8StringSerializer

object StreamWriteExample extends ZIOAppDefault {

  val stringWriterSettings = WriterSettingsBuilder()
    .eventWriterConfigBuilder(_.enableLargeEvents(true))
    .withSerializer(new UTF8StringSerializer)

  private def testStream(a: Int, b: Int): ZStream[Any, Nothing, String] = ZStream
    .fromIterable(a to b)
    .map(i => f"$i%04d_name $i")

  val program = for {
    _ <- ZIO.debug("Writing to stream")
    _ <- testStream(1, 10).tap(p => ZIO.debug(p.toString())).run(PravegaStream.sink("a-stream", stringWriterSettings))
    _ <- ZIO.debug("Done")

  } yield ()

  override def run: ZIO[Scope, Throwable, Unit] = program
    .provideSome[Scope](PravegaClientConfig.live, PravegaStream.fromScope("a-scope"))

}
