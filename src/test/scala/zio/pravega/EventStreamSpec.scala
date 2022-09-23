package zio.pravega

import zio._

import zio.test._
import zio.test.Assertion._
//import zio.test.TestAspect._

object EventStreamSpec extends SharedPravegaContainerSpec("event-streaming") {

  override def spec: Spec[Environment with TestEnvironment with Scope, Any] =
    scopedSuite(
      test("EventStream support timeouts")(
        for {

//          _ <- PravegaAdmin.openReaderGroup("zio-scope", groupName)

          _ <- PravegaAdmin.createStream(aScope, "s1", streamConfig(2))

          _ <- PravegaAdmin
            .createReaderGroup(
              aScope,
              "g1",
              "s1"
            )

          sink1 <- sink("s1")
          sink2 <- sinkTx("s1")

          _ <- testStream(0, 10).run(sink1)

          _ <- (ZIO.attemptBlocking(Thread.sleep(6000)) *> ZIO.logDebug(
            "(( Re-start producing ))"
          ) *> testStream(10, 20)
            .run(sink2)).fork

          stream1 <- PravegaStream.eventStream("g1", personReaderSettings)
          stream2 <- PravegaStream.eventStream("g1", personReaderSettings)

          fiber1 <- stream1
            .take(10)
            .tap(m =>
              ZIO
                .when(m.isCheckpoint())(
                  ZIO.debug(s"🏁 ${m.getCheckpointName()}")
                )
            )
            .runFold(0)((s, _) => s + 1)
            .fork
          fiber2 <- stream2
            .take(10)
            .tap(m =>
              ZIO
                .when(m.isCheckpoint())(
                  ZIO.debug(s"🏁 ${m.getCheckpointName()}")
                )
            )
            .runFold(0)((s, _) => s + 1)
            .fork

          count1 <- fiber1.join
          count2 <- fiber2.join
          count = count1 + count2
        } yield assert(count)(equalTo(20))
      )
    )
}
