package zio.pravega

import zio._

import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._

object StreamTxSpec extends SharedPravegaContainerSpec("streaming-tx") {

  import CommonTestSettings._

  override def spec: Spec[Environment with TestEnvironment, Any] =
    scopedSuite(
      suite("Tx Stream support")(
        test("Stream support timeouts") {
          for {
            _ <- PravegaAdmin.createStream(aScope, "s1", streamConfig(2))

            _ <- createGroup("g1", "s1")

            sink1 = sink("s1", true)
            sink2 = sinkTx("s1", true)
            _ <- testStream(0, 50).run(sink1).fork
            stream1 = PravegaStream
              .stream("g1", personReaderSettings)
            fib1 <- stream1
              .take(50)
              .runCount
              .fork
            stream2 = PravegaStream
              .stream("g1", personReaderSettings)
            fib2 <- stream2
              .take(50)
              .runCount
              .fork
            _ <- (ZIO.sleep(2000.millis) *> ZIO.logDebug(
              "(( Re-start producing ))"
            ) *> testStream(50, 100)
              .run(sink2)).fork
            count1 <- fib1.join
            count2 <- fib2.join
            count = count1 + count2
            _ <- ZIO.logDebug(s"count $count1 + $count2 = $count")
          } yield assert(count)(equalTo(100L))
        } @@ withLiveClock,
        test("Roll back sinks") {
          for {
            _ <- PravegaAdmin.createStream(aScope, "s2", streamConfig(1))

            sink0 = sink("s2")
            sinkAborted = sinkTx("s2")

            _ <- createGroup("g2", "s2")
            _ <- testStream(1, 100)
              .tap(p => ZIO.when(p.age % 50 == 0)(ZIO.fail("Boom")))
              .run(sinkAborted)
              .sandbox
              .ignore
            _ <- testStream(50, 100).run(sink0)

            stream = PravegaStream.stream("g2", personReaderSettings)
            count <- stream
              .take(50)
              .filter(_.age < 50)
              .runCount

          } yield assert(count)(equalTo(0L))
        }
      )
    )

}
