package zio.pravega.test

import io.pravega.client.stream.impl.UTF8StringSerializer

import zio.Console._
import zio._
import zio.stream._
import zio.pravega._

import io.pravega.client.stream.StreamConfiguration
import io.pravega.client.stream.ScalingPolicy

object TestZioApp extends ZIOAppDefault {

  override def run: ZIO[Environment with ZEnv with ZIOAppArgs, Any, Any] =
    program
      .provideCustom(
        Pravega.layer(
          scope,
          writterSettings.clientConfig
        ) ++ PravegaAdmin.layer(
          writterSettings.clientConfig
        )
      )
      .exitCode

  val scope = "zio-scope"
  val streamName = "zio-stream"
  val groupName = "coco1"

  val n = 10

  val writterSettings =
    WriterSettingsBuilder()
      .withSerializer(new UTF8StringSerializer)

  val readerSettings =
    ReaderSettingsBuilder()
      .withSerializer(new UTF8StringSerializer)

  val clientConfig = writterSettings.clientConfig

  def initScopeAndStream
      : ZIO[PravegaAdminService with Console, Throwable, Unit] =
    for {
      scopeCreated <- PravegaAdminService(_.createScope(scope))
      _ <- ZIO.when(scopeCreated)(printLine(s"Scope $scope just created"))
      streamCreated <- PravegaAdminService(
        _.createStream(
          streamName,
          StreamConfiguration.builder
            .scalingPolicy(ScalingPolicy.fixed(8))
            .build,
          scope
        )
      )
      _ <- ZIO.when(streamCreated)(
        printLine(s"Stream $streamName just created")
      )
    } yield ()

  def testStream(a: Int, b: Int): ZStream[Any, Nothing, String] =
    Stream.fromIterable(a until b).map(i => s"ZIO Message $i")

  private val writeToAndConsumeStream: ZIO[
    Clock with Console with PravegaService with PravegaAdminService,
    Any,
    Int
  ] =
    for {
      sink <- PravegaService(_.pravegaSink(streamName, writterSettings))
      _ <- testStream(0, 10).run(sink)
      _ <- (ZIO.sleep(2.seconds) *> printLine(
        "(( Re-start producing ))"
      ) *> testStream(10, 20).run(sink)).fork
      _ <- PravegaAdminService(
        _.readerGroup(
          scope,
          groupName,
          streamName
        )
      )

      stream <- PravegaService(_.pravegaStream(groupName, readerSettings))
      _ <- printLine("Consuming...")
      count <- stream
        .take(n.toLong * 2)
        .tap(e => printLine(s"ZStream of [$e]"))
        .runFold(0)((s, _) => s + 1)
      _ <- printLine(s"Consumed $count messages")

    } yield count

  val program = for {
    _ <- initScopeAndStream
    count <- writeToAndConsumeStream
  } yield count

}
