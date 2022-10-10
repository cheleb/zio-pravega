package zio.pravega.docs

import zio._
import zio.pravega._
import zio.stream._

import io.pravega.client.stream.impl.UTF8StringSerializer

object StreamWriteExample extends ZIOAppDefault {

  val clientConfig = PravegaClientConfig.default

  val stringWriterSettings = WriterSettingsBuilder()
    .eventWriterConfigBuilder(_.enableLargeEvents(true))
    .withSerializer(new UTF8StringSerializer)

  private def testStream(a: Int, b: Int): ZStream[Any, Nothing, String] = ZStream
    .fromIterable(a to b)
    .map(i => f"$i%04d_name $i")

  val program = for {

    _ <- testStream(1, 10).tap(p => ZIO.debug(p.toString())).run(PravegaStream.sink("a-stream", stringWriterSettings))
    _ <- ZIO.log("Coucou")

  } yield ()

  override def run: ZIO[Any, Throwable, Unit] = program
    .provide(Scope.default, PravegaStream.fromScope("a-scope", clientConfig))

}
