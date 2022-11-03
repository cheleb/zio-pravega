package zio.pravega

import zio._

import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._
import java.util.concurrent.Executors

import io.pravega.client.stream.notifications.Listener
import io.pravega.client.stream.notifications.SegmentNotification
import zio.pravega.admin._

/**
 * Test the Pravega Stream API.
 *
 * autoScale.cooldown.time.seconds to 2 seconds
 */
object SegmentListernerSpec extends SharedPravegaContainerSpec("segment-listener") {

  import CommonTestSettings._

  override def spec: Spec[Environment with TestEnvironment, Any] = scopedSuite(test("Stream rebalance") {
    for {
      _ <- PravegaStreamManager.createStream(aScope, "s1", dynamicStreamConfig(10, 2, 1))

      _ <- PravegaReaderGroupManager.createReaderGroup("g1", "s1")

      group    <- PravegaReaderGroupManager.openReaderGroup("g1")
      executor <- ZIO.succeed(Executors.newScheduledThreadPool(1)).withFinalizerAuto
      not       = group.getSegmentNotifier(executor)
      _ <- ZIO.attemptBlocking {
             not.registerListener(new Listener[SegmentNotification] {
               def onNotification(
                 segmentNotification: io.pravega.client.stream.notifications.SegmentNotification
               ): Unit =
                 println(s"SegmentNotification $segmentNotification")
             })
           }
      stream1 = PravegaStream.stream("g1", personReaderSettings)
      fib1   <- stream1.take(1000000).runCount.fork
      sink1   = sink("s1")

      _ <- (ZIO.sleep(2000.millis) *> testStream(0, 1000).run(sink1) *> ZIO.logInfo("1000")).repeatN(1000).fork

      count <- fib1.join

      _ <- ZIO.logDebug(s"count = $count")
    } yield assert(count)(equalTo(1000000L))
  } @@ withLiveClock @@ ignore)

}
