package zio.pravega

import zio._
import zio.stream._
import zio.test._
import zio.test.Assertion._

trait StreamSpec {
  this: ZIOSpec[
    PravegaStreamService & PravegaAdminService & PravegaTableService
  ] =>

  val n = 10

  import CommonSettings._

  private def testStream(a: Int, b: Int): ZStream[Any, Nothing, String] =
    ZStream.fromIterable(a until b).map(i => f"$i%04d ZIO Message")

  def streamSuite(
      pravegaStreamName: String,
      groupName: String
  ) = {

    val writeToAndConsumeStream = ZIO.scoped {
      for {
        sink <- PravegaStreamService.sink(pravegaStreamName, writterSettings)
        _ <- testStream(0, 10).run(sink)
        _ <- (ZIO.attemptBlocking(Thread.sleep(2000)) *> ZIO.logDebug(
          "(( Re-start producing ))"
        ) *> testStream(10, 20).run(sink)).fork

        stream1 <- PravegaStreamService.stream(groupName, readerSettings)
        stream2 <- PravegaStreamService.stream(groupName, readerSettings)

        _ <- ZIO.logDebug("Consuming...")
        count1Fiber <- stream1
          .take(n.toLong)
          .tap(m => ZIO.logDebug(m))
          .runFold(0)((s, _) => s + 1)
          .fork

        count2Fiber <- stream2
          .take(n.toLong)
          .tap(m => ZIO.logDebug(m))
          .runFold(0)((s, _) => s + 1)
          .fork

        count1 <- count1Fiber.join
        count2 <- count2Fiber.join

      } yield count1 + count2
    }

    suite("Stream")(
      test("publish and consume")(
        writeToAndConsumeStream
          .map(count => assert(count)(equalTo(20)))
      )
    )
  }
}