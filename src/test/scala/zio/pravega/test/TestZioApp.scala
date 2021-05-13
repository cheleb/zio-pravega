package zio.pravega.test

import io.pravega.client.stream.impl.UTF8StringSerializer

import zio.Console._
import zio._
import zio.stream._
import zio.pravega._
import zio.Pravega._
import io.pravega.client.stream.StreamConfiguration
import io.pravega.client.stream.ScalingPolicy

object TestZioApp extends App {

  val group = "coco1"

  val n = 10

  val writterSettings =
    WriterSettingsBuilder()
      .withSerializer(new UTF8StringSerializer)

  val readerSettings =
    ReaderSettingsBuilder()
      .withSerializer(new UTF8StringSerializer)

  val clientConfig = writterSettings.clientConfig

  def initScopeAndStream: ZIO[Has[Console], Throwable, Boolean] = PravegaAdmin.streamManager(clientConfig).use {
    streamManager =>
      ZIO.attemptBlocking(streamManager.createScope("zio-scope")) *> ZIO
        .attemptBlocking(
          streamManager.createStream(
            "zio-scope",
            "zio-stream",
            StreamConfiguration.builder
              .scalingPolicy(ScalingPolicy.fixed(8))
              .build
          )
        ) <* printLine("Scope and stream inited")

  }

  def testStream(a: Int, b: Int): ZStream[Any, Nothing, String] =
    Stream.fromIterable(a until b).map(i => s"ZIO Message $i")

  private val writeToAndConsumeStream: ZIO[Has[Clock] with Has[Console] with Has[Service] with Has[Console], Any, Int] =
    for {
      sink <- pravegaSink("zio-stream", writterSettings)
      _    <- testStream(0, 10).run(sink)
      _ <- (ZIO.sleep(2.seconds) *> printLine("(( Re-start producing ))") *>
            testStream(10, 20).run(sink)).fork
      _ <- readerGroup(group, readerSettings, "zio-stream")

      stream <- pravegaStream(group, readerSettings)
      _      <- printLine("Consuming...")
      count <- stream
                .take(n.toLong * 2)
                .tap(e => printLine(s"ZStream of [$e]"))
                .fold(0)((s, _) => s + 1)
      _ <- printLine(s"Consumed $count messages")

    } yield count

  val program = for {
    _ <- initScopeAndStream
    count <- writeToAndConsumeStream
              .provideCustomLayer(Pravega.live("zio-scope", writterSettings.clientConfig))
  } yield count

  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    program.exitCode
}
