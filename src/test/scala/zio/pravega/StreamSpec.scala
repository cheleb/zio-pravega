package zio.pravega

import zio._

import zio.test._
import zio.test.Assertion._

object StreamSpec extends SharedPravegaContainerSpec("streaming-timeout") {

  import CommonTestSettings._

  override def spec: Spec[Environment with TestEnvironment, Any] =
    scopedSuite(test("Stream support timeouts") {
      for {
        _ <- PravegaAdmin.createStream(aScope, "s1", streamConfig(2))

        _ <- PravegaAdmin
          .createReaderGroup(
            aScope,
            "g1",
            "s1"
          )

        sink1 <- sink("s1")
        sink2 <- sinkTx("s1")
        _ <- testStream(0, 50).run(sink1).fork
        stream1 <- PravegaStream
          .stream("g1", personReaderSettings)
        fib1 <- stream1
          .take(50)
          .runCount
          .fork
        stream2 <- PravegaStream
          .stream("g1", personReaderSettings)
        fib2 <- stream2
          .take(50)
          .runCount
          .fork
        _ <- (ZIO.attemptBlocking(Thread.sleep(2000)) *> ZIO.logDebug(
          "(( Re-start producing ))"
        ) *> testStream(50, 100)
          .run(sink2)).fork
        count1 <- fib1.join
        count2 <- fib2.join
        count = count1 + count2
        _ <- ZIO.logDebug(s"count $count1 + $count2 = $count")
      } yield assert(count)(equalTo(100L))
    })

}
