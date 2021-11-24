package zio.pravega.test

import zio.test.TestClock
import zio.test.Assertion._
import zio.pravega.WriterSettingsBuilder
import io.pravega.client.stream.impl.UTF8StringSerializer
import zio.pravega.ReaderSettingsBuilder

import zio.stream._
import zio._
import zio.test._
import zio.Console
import zio.Console._
import io.pravega.client.admin.StreamManager
import io.pravega.client.stream.StreamConfiguration
import io.pravega.client.stream.ScalingPolicy
import zio.stream.ZStream
//import zio.pravega._
import zio.Pravega._

import zio.PravegaAdmin

// Pravega docker container only works under linux
object PravegaStreamSpec extends PravegaIT { // */ DefaultRunnableSpec {

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

  def initScopeAndStream: ZIO[StreamManager with Console, Throwable, Unit] =
    for {
      scopeCreated <- PravegaAdmin.createScope(scope)
      _ <- ZIO.when(scopeCreated)(printLine(s"Scope $scope just created"))
      streamCreated <- PravegaAdmin.createStream(
        streamName,
        StreamConfiguration.builder
          .scalingPolicy(ScalingPolicy.fixed(8))
          .build,
        scope
      )
      _ <- ZIO.when(streamCreated)(
        printLine(s"Stream $streamName just created")
      )
    } yield ()

  def testStream(a: Int, b: Int): ZStream[Any, Nothing, String] =
    Stream.fromIterable(a until b).map(i => s"ZIO Message $i")

  private val writeToAndConsumeStream =
    // : ZIO[Has[TestClock] with Has[Console] with Has[Service] with Has[Console], Any, Int] =
    for {
      sink <- pravegaSink(streamName, writterSettings)
      _ <- testStream(0, 10).run(sink)
      _ <- (ZIO.sleep(2.seconds) *> printLine(
        "(( Re-start producing ))"
      ) *> testStream(10, 20).run(sink)).fork
      _ <- TestClock.adjust(2.seconds)
      _ <- PravegaAdmin.readerGroup(
        scope,
        groupName,
        readerSettings,
        streamName
      )
      stream <- pravegaStream(groupName, readerSettings)
      _ <- printLine("Consuming...")
      count <- stream
        .take(n.toLong * 2)
        .tap(e => printLine(s"ZStream of [$e]"))
        .runFold(0)((s, _) => s + 1)
      _ <- printLine(s"Consumed $count messages")

    } yield count

  val program = for {
    _ <- initScopeAndStream.provideCustom(
      PravegaAdmin.streamManager(clientConfig).toLayer
    )
    count <- writeToAndConsumeStream
    //  .provideCustomLayer(Pravega.live(scope, writterSettings.clientConfig))

  } yield count

  val spec: Spec[Clock with Console with System with Random, TestFailure[
    Any
  ], TestSuccess] =
    suite("Prvavega")(
      zio.test.test("publish and consume") {

        assertM(
          program.provideCustom(
            Pravega.live(
              scope,
              writterSettings.clientConfig
            ) ++ zio.test.testEnvironment
          )
        )(equalTo(20))
      }
    )

}
