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

import io.pravega.client.stream.StreamConfiguration
import io.pravega.client.stream.ScalingPolicy
import zio.stream.ZStream
import zio.pravega._

import zio._

import zio.Random

// Pravega docker container only works under linux
object PravegaStreamSpec extends PravegaIT { // DefaultRunnableSpec {

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

  def initScopeAndStream =
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
    PravegaStreamService with PravegaAdminService with Console with TestClock with Clock,
    Throwable,
    Int
  ] =
    for {
      sink <- PravegaService(_.sink(streamName, writterSettings))
      _ <- testStream(0, 10).run(sink)
      _ <- (ZIO.sleep(2.seconds) *> printLine(
        "(( Re-start producing ))"
      ) *> testStream(10, 20).run(sink)).fork
      _ <- adjust(2.seconds)
      _ <- PravegaAdminService(
        _.readerGroup(
          scope,
          groupName,
          streamName
        )
      )
      stream <- PravegaService(_.stream(groupName, readerSettings))
      _ <- printLine("Consuming...")
      count <- stream
        .take(n.toLong * 2)
        .tap(e => printLine(s"ZStream of [$e]"))
        .runFold(0)((s, _) => s + 1)

      _ <- printLine(s"Consumed $count messages")

    } yield count

  val program: ZIO[
    zio.ZEnv with PravegaStreamService with PravegaAdminService with Console with TestClock with Clock with Random,
    Throwable,
    Int
  ] =
    for {
      _ <- initScopeAndStream
      count <- writeToAndConsumeStream

    } yield count

  val spec: Spec[
    Clock with TestClock with Console with System with Random,
    TestFailure[
      Throwable
    ],
    TestSuccess
  ] =
    suite("Pravega")(
      zio.test.test("publish and consume") {
        assertM(
          program.provideCustom(
            Pravega.layer(
              scope,
              writterSettings.clientConfig
            ) ++ PravegaAdmin.layer(
              writterSettings.clientConfig
            ) ++ zio.test.testEnvironment
          )
        )(equalTo(20))
      }
    )

}
