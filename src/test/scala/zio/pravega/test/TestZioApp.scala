package zio.pravega.test

import io.pravega.client.stream.impl.UTF8StringSerializer

import zio.Console._
import zio._
import zio.stream._
import zio.pravega._
import zio.Pravega._
import io.pravega.client.stream.StreamConfiguration
import io.pravega.client.stream.ScalingPolicy
import io.pravega.client.admin.StreamManager

object TestZioApp extends ZIOAppDefault {

  override def run: ZIO[Environment with ZEnv with Has[ZIOAppArgs], Any, Any] = program.exitCode

  val scope      = "zio-scope"
  val streamName = "zio-stream"
  val groupName  = "coco1"

  val n = 10

  val writterSettings =
    WriterSettingsBuilder()
      .withSerializer(new UTF8StringSerializer)

  val readerSettings =
    ReaderSettingsBuilder()
      .withSerializer(new UTF8StringSerializer)

  val clientConfig = writterSettings.clientConfig

  def initScopeAndStream: ZIO[Has[StreamManager] with Has[Console], Throwable, Unit] =
    for {
      scopeCreated <- PravegaAdmin.createScope(scope)
      _            <- ZIO.when(scopeCreated)(printLine(s"Scope $scope just created"))
      streamCreated <- PravegaAdmin.createStream(
                        streamName,
                        StreamConfiguration.builder
                          .scalingPolicy(ScalingPolicy.fixed(8))
                          .build,
                        scope
                      )
      _ <- ZIO.when(streamCreated)(printLine(s"Stream $streamName just created"))
    } yield ()

  def testStream(a: Int, b: Int): ZStream[Any, Nothing, String] =
    Stream.fromIterable(a until b).map(i => s"ZIO Message $i")

  private val writeToAndConsumeStream: ZIO[Has[Clock] with Has[Console] with Has[Service] with Has[Console], Any, Int] =
    for {
      sink <- pravegaSink(streamName, writterSettings)
      _    <- testStream(0, 10).run(sink)
      _ <- (ZIO.sleep(2.seconds) *> printLine("(( Re-start producing ))") *>
            testStream(10, 20).run(sink)).fork
      _ <- PravegaAdmin.readerGroup(scope, groupName, readerSettings, streamName)

      stream <- pravegaStream(groupName, readerSettings)
      _      <- printLine("Consuming...")
      count <- stream
                .take(n.toLong * 2)
                .tap(e => printLine(s"ZStream of [$e]"))
                .fold(0)((s, _) => s + 1)
      _ <- printLine(s"Consumed $count messages")

    } yield count

  val program = for {
    _ <- initScopeAndStream.provideCustomLayer(PravegaAdmin.streamManager(clientConfig).toLayer)
    count <- writeToAndConsumeStream
              .provideCustomLayer(Pravega.live(scope, writterSettings.clientConfig))
  } yield count

}
