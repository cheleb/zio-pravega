package zio.pravega

import zio._

import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.pravega.admin._

object StreamSpec extends SharedPravegaContainerSpec("streaming-timeout") {

  import CommonTestSettings._

  override def spec: Spec[Environment with TestEnvironment, Any] = scopedSuite(test("Stream support timeouts") {
    for {
      _ <- PravegaStreamManager.createStream(aScope, "s1", staticStreamConfig(2))

      _ <- PravegaReaderGroupManager.createReaderGroup("g1", "s1")

      sink1     = sink("s1")
      sink2     = sinkTx("s1")
      writeFlow = PravegaStream.writeFlow("s1", personStreamWriterSettings)
      stream1   = PravegaStream.stream("g1", personReaderSettings)
      stream2   = PravegaStream.stream("g1", personReaderSettings)

      _      <- testStream(0, 50).run(sink1).fork
      fib1   <- stream1.take(75).runCount.fork
      fib2   <- stream2.take(75).runCount.fork
      _      <- (ZIO.sleep(2000.millis) *> ZIO.logDebug("(( Re-start producing ))") *> testStream(50, 100).run(sink2)).fork
      fib3   <- testStream(100, 150).via(writeFlow).runDrain.fork
      count1 <- fib1.join
      count2 <- fib2.join
      _      <- fib3.join
      count   = count1 + count2
      _      <- ZIO.logDebug(f"count $count1%d + $count2%d = $count%d")
    } yield assert(count)(equalTo(150L))
  } @@ withLiveClock)

}
