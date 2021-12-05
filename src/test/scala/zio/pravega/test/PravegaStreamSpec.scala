package zio.pravega.test

import zio.test.Assertion._
import zio.pravega.WriterSettingsBuilder
import io.pravega.client.stream.impl.UTF8StringSerializer
import zio.pravega.ReaderSettingsBuilder

import zio.stream._

import zio.test._
import zio.test.TestClock._
import zio.Console
import zio.Console._
import io.pravega.client.admin.StreamManager
import io.pravega.client.stream.StreamConfiguration
import io.pravega.client.stream.ScalingPolicy
import zio.stream.ZStream
import zio.pravega._

import zio._

import zio.Random

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
    for {
      sink <- PravegaService(_.pravegaSink(streamName, writterSettings))
      _ <- testStream(0, 10).run(sink)
      _ <- (ZIO.sleep(2.seconds) *> printLine(
        "(( Re-start producing ))"
      ) *> testStream(10, 20).run(sink)).fork
      _ <- adjust(2.seconds)
      _ <- PravegaAdmin.readerGroup(
        scope,
        groupName,
        readerSettings,
        streamName
      )
      stream <- PravegaService(_.pravegaStream(groupName, readerSettings))
      _ <- printLine("Consuming...")
      count <- stream
        .take(n.toLong * 2)
        .tap(e => printLine(s"ZStream of [$e]"))
        .runFold(0)((s, _) => s + 1)

      _ <- printLine(s"Consumed $count messages")

    } yield count

  val program
      : ZIO[zio.ZEnv with PravegaService with TestClock, Throwable, Int] =
    for {
      _ <- initScopeAndStream.provideCustom(
        PravegaAdmin.streamManager(clientConfig).toLayer
      )
      count <- writeToAndConsumeStream

    } yield count

  val spec: Spec[Clock with Console with System with Random, TestFailure[
    Throwable
  ], TestSuccess] =
    suite("Prvavega")(
      zio.test.test("publish and consume") {
        assertM(
          program.provideCustom(
            Pravega.layer(
              scope,
              writterSettings.clientConfig
            ) ++ zio.test.testEnvironment
          )
        )(equalTo(20))
      }
    )

}
