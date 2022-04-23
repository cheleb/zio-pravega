package zio.pravega

import zio._
import zio.stream._
import zio.test._
import zio.test.Assertion._

import io.pravega.client.stream.impl.UTF8StringSerializer

trait StreamSpec {
  this: ZIOSpec[
    PravegaStreamService & PravegaAdminService & PravegaTableService
  ] =>

  val n = 10

  val writterSettings =
    WriterSettingsBuilder()
      .withSerializer(new UTF8StringSerializer)

  val readerSettings =
    ReaderSettingsBuilder()
      .withSerializer(new UTF8StringSerializer)

  val clientConfig = writterSettings.clientConfig

  private def testStream(a: Int, b: Int): ZStream[Any, Nothing, String] =
    Stream.fromIterable(a until b).map(i => f"$i%04d ZIO Message")

  def streamSuite(
      pravegaStreamName: String,
      groupName: String
  ): Spec[PravegaStreamService, TestFailure[
    Throwable
  ], TestSuccess] = {

    val writeToAndConsumeStream = ZIO.scoped {
      for {
        sink <- PravegaStream(_.sink(pravegaStreamName, writterSettings))
        _ <- testStream(0, 10).run(sink)
        _ <- (ZIO.attemptBlocking(Thread.sleep(2000)) *> ZIO.logDebug(
          "(( Re-start producing ))"
        ) *> testStream(10, 20).run(sink)).fork

        stream <- PravegaStream(_.stream(groupName, readerSettings))
        _ <- ZIO.logDebug("Consuming...")
        count <- stream
          .take(n.toLong * 2)
          .tap(m => ZIO.logDebug(m))
          .runFold(0)((s, _) => s + 1)

        _ <- ZIO.logDebug(s"Consumed $count messages")

      } yield count
    }

    suite("Stream")(
      test("publish and consume")(
        writeToAndConsumeStream
          .map(count => assert(count)(equalTo(20)))
      )
    )
  }
}
