package zio.pravega

import zio.test.Assertion._
import zio.pravega.WriterSettingsBuilder
import io.pravega.client.stream.impl.UTF8StringSerializer
import zio.pravega.ReaderSettingsBuilder

import zio.stream._

import zio._
import zio.test._
import zio.test.TestAspect._
import zio.test.TestClock._

import zio.Console._

import io.pravega.client.stream.StreamConfiguration
import io.pravega.client.stream.ScalingPolicy
import zio.stream.ZStream
import zio.pravega._

import zio.pravega.test.PravegaContainer

object PravegaITs extends ZIOSpec[PravegaStreamService & PravegaAdminService] {

  val pravegaScope = "zio-scope"
  val pravegaStreamName = "zio-stream"

  val layer: ZLayer[Scope, TestFailure[
    Nothing
  ], PravegaAdmin & PravegaStreamService] =
    PravegaContainer.pravega >>> PravegaContainer.clientConfig >>> (PravegaAdmin.layer ++
      PravegaStream.layer(pravegaScope).mapError(t => TestFailure.die(t)))

  val groupName = "coco1"

  val n = 10

  val writterSettings =
    WriterSettingsBuilder()
      .withSerializer(new UTF8StringSerializer)

  val readerSettings =
    ReaderSettingsBuilder()
      .withSerializer(new UTF8StringSerializer)

  val clientConfig = writterSettings.clientConfig

  def testStream(a: Int, b: Int): ZStream[Any, Nothing, String] =
    Stream.fromIterable(a until b).map(i => s"ZIO Message $i")

  private val writeToAndConsumeStream: ZIO[
    Scope & PravegaStreamService & PravegaAdminService,
    Throwable,
    Int
  ] =
    for {
      sink <- PravegaService(_.sink(pravegaStreamName, writterSettings))
      _ <- testStream(0, 10).run(sink)
      _ <- (ZIO.sleep(2.seconds) *> printLine(
        "(( Re-start producing ))"
      ) *> testStream(10, 20).run(sink)).fork

      _ <- PravegaAdminService(
        _.readerGroup(
          pravegaScope,
          groupName,
          pravegaStreamName
        )
      )
      stream <- PravegaService(_.stream(groupName, readerSettings))
      _ <- printLine("Consuming...")
      count <- stream
        .take(n.toLong * 2)
        .tap(e =>
          adjust(200.millis) *>
            printLine(s"ZStream of [$e]")
        )
        .runFold(0)((s, _) => s + 1)

      _ <- printLine(s"Consumed $count messages")

    } yield count

  def spec =
    suite("Pravega")(
      test("Scope created once")(
        PravegaAdminService(_.createScope(pravegaScope))
          .map(once => assert(once)(isTrue))
      ),
      test("Scope skip twice")(
        PravegaAdminService(_.createScope(pravegaScope))
          .map(twice => assert(twice)(isFalse))
      ),
      test("Stream created once")(
        PravegaAdminService(
          _.createStream(
            pravegaStreamName,
            StreamConfiguration.builder
              .scalingPolicy(ScalingPolicy.fixed(8))
              .build,
            pravegaScope
          )
        )
          .map(once => assert(once)(isTrue))
      ),
      test("Stream creation skiped")(
        PravegaAdminService(
          _.createStream(
            pravegaStreamName,
            StreamConfiguration.builder
              .scalingPolicy(ScalingPolicy.fixed(8))
              .build,
            pravegaScope
          )
        )
          .map(twice => assert(twice)(isFalse))
      ),
      test("publish and consume")(
        writeToAndConsumeStream
          .map(count => assert(count)(equalTo(20)))
      )
    ) @@ sequential

}
