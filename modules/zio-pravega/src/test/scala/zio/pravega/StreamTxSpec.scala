package zio.pravega

import zio._

import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.pravega.admin._
import java.util.UUID

object StreamTxSpec extends SharedPravegaContainerSpec("streaming-tx") {

  import CommonTestSettings._

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  override def spec: Spec[Environment & TestEnvironment, Any] = scopedSuite(
    suite("Tx Stream support")(
      test("Stream support timeouts") {
        val aStreamName = "stream-timeout"
        val aGroupName  = s"$aStreamName-group"
        for {
          _ <- PravegaStreamManager.createStream(aScope, aStreamName, staticStreamConfig(2))

          _ <- createGroup(aGroupName, aStreamName)

          sink1   = sink(aStreamName, true)
          sink2   = sinkTx(aStreamName, true)
          _      <- testStream(0, 50).run(sink1).fork
          stream1 = PravegaStream.stream(aGroupName, personReaderSettings)
          fib1   <- stream1.take(50).runCount.fork
          stream2 = PravegaStream.stream(aGroupName, personReaderSettings)
          fib2   <- stream2.take(50).runCount.fork
          _ <-
            (ZIO.sleep(2000.millis) *> ZIO.logDebug("(( Re-start producing ))") *> testStream(50, 100).run(sink2)).fork
          count1 <- fib1.join
          count2 <- fib2.join
          count   = count1 + count2
          _      <- ZIO.logDebug(f"count $count1%d + $count2%d = $count%d")
        } yield assert(count)(equalTo(100L))
      } @@ withLiveClock,
      test("Roll back sinks") {
        val aStreamName = "stream-rollback"
        val aGroupName  = s"$aStreamName-group"
        for {
          _ <- PravegaStreamManager.createStream(aScope, aStreamName, staticStreamConfig(1))

          sink0       = sink(aStreamName)
          sinkAborted = sinkTx(aStreamName)

          _ <- createGroup(aGroupName, aStreamName)
          _ <- testStream(1, 100)
                 .tap(p => ZIO.when(p.age.equals(50))(ZIO.die(FakeException("Boom"))))
                 .run(sinkAborted)
                 .sandbox
                 .ignore
          _ <- testStream(50, 100).run(sink0)

          stream = PravegaStream.stream(aGroupName, personReaderSettings)
          count <- stream.take(50).filter(_.age < 50).runCount

        } yield assert(count)(equalTo(0L))
      },
      suite("Distributed tx success")(
        test("2 writers in a same transaction succeed") {
          val aStreamName = "tx-2-writer-success"
          val aGroupName  = s"$aStreamName-group"
          for {
            _ <- PravegaStreamManager.createStream(aScope, aStreamName, staticStreamConfig(1))

            txUUID <- PravegaStream.writeTx(aStreamName, personStreamWriterSettings)

            txSink = PravegaStream.sharedTransactionalSink(aStreamName, txUUID, personStreamWriterSettings, false)

            _ <- testStream(0, 50).run(txSink)

            txSink2 = PravegaStream.sharedTransactionalSink(aStreamName, txUUID, personStreamWriterSettings, true)
            _      <- testStream(0, 50).run(txSink2).fork
            _      <- createGroup(aGroupName, aStreamName)
            stream  = PravegaStream.stream(aGroupName, personReaderSettings)
            count  <- stream.take(100).runCount

          } yield assert(count)(equalTo(100L))
        },
        test("3 writers in a same transaction succeed") {
          val aStreamName = "tx-3-writer-success"
          val aGroupName  = s"$aStreamName-group"
          for {
            _ <- PravegaStreamManager.createStream(aScope, aStreamName, staticStreamConfig(1))

            txUUID <- PravegaStream.writeTx(aStreamName, personStreamWriterSettings)

            txSink1 = PravegaStream.sharedTransactionalSink(aStreamName, txUUID, personStreamWriterSettings, false)

            _ <- testStream(0, 50).run(txSink1)

            txSink2 = PravegaStream.sharedTransactionalSink(aStreamName, txUUID, personStreamWriterSettings, false)
            _      <- testStream(0, 50).run(txSink2)
            txSink3 = PravegaStream.sharedTransactionalSink(aStreamName, txUUID, personStreamWriterSettings, true)
            _      <- testStream(0, 50).run(txSink3)
            // Read from the stream
            _     <- createGroup(aGroupName, aStreamName)
            stream = PravegaStream.stream(aGroupName, personReaderSettings)
            count <- stream.take(150).runCount

          } yield assert(count)(equalTo(150L))
        },
        test("3 writers in a same transaction succeed, already closed tx") {
          val aStreamName = "tx-3-writer-success-closed"
          val aGroupName  = s"$aStreamName-group"
          for {
            _      <- PravegaStreamManager.createStream(aScope, aStreamName, staticStreamConfig(1))
            txUUID <- PravegaStream.writeTx(aStreamName, personStreamWriterSettings)
            txSink  = PravegaStream.sharedTransactionalSink(aStreamName, txUUID, personStreamWriterSettings, false)
            _      <- testStream(0, 50).run(txSink)

            txSink2 = PravegaStream.sharedTransactionalSink(aStreamName, txUUID, personStreamWriterSettings, true)
            _      <- testStream(0, 50).run(txSink2)
            txSink3 = PravegaStream.sharedTransactionalSink(aStreamName, txUUID, personStreamWriterSettings, true)
            _      <- testStream(0, 50).run(txSink3).fork
            // Read from the stream
            _     <- createGroup(aGroupName, aStreamName)
            stream = PravegaStream.stream(aGroupName, personReaderSettings)
            count <- stream.take(150).runCount.timeout(1.seconds)

          } yield assert(count)(equalTo(None))
        } @@ withLiveClock
      ) @@ sequential,
      suite("Distributed tx fail if one of them fails")(
        test("First tx fail") {
          val aStreamName = "tx-first-fail"
          val aGroupName  = s"$aStreamName-group"
          for {
            _ <- PravegaStreamManager.createStream(aScope, aStreamName, staticStreamConfig(1))

            txSink = PravegaStream.newSharedTransactionalSink(aStreamName, personStreamWriterSettings)
            _ <- testStream(0, 50)
                   .tap(p => ZIO.when(p.age.equals(25))(ZIO.die(FakeException("Boom"))))
                   .run(txSink)
                   .sandbox
                   .ignore

            _     <- createGroup(aGroupName, aStreamName)
            stream = PravegaStream.stream(aGroupName, personReaderSettings)
            count <- stream.take(1).runCount.timeout(1.seconds)

          } yield assert(count)(equalTo(None))
        },
        test("Second tx fail") {
          val aStreamName = "tx-second-fail"
          val aGroupName  = s"$aStreamName-group"
          for {
            _ <- PravegaStreamManager.createStream(aScope, aStreamName, staticStreamConfig(1))

            txUUID <- PravegaStream.writeTx(aStreamName, personStreamWriterSettings)
            txSink  = PravegaStream.sharedTransactionalSink(aStreamName, txUUID, personStreamWriterSettings, false)
            _ <- testStream(0, 50)
                   .tap(p => ZIO.when(p.age.equals(25))(ZIO.die(FakeException("Boom"))))
                   .run(txSink)
                   .sandbox
                   .ignore

            _     <- createGroup(aGroupName, aStreamName)
            stream = PravegaStream.stream(aGroupName, personReaderSettings)
            count <- stream.take(1).runCount.timeout(1.seconds)

          } yield assert(count)(equalTo(None))
        },
        test("2 writers second fail") {
          val aStreamName = "tx-2writer-ssecond-fail"
          val aGroupName  = s"$aStreamName-group"
          for {
            _ <- PravegaStreamManager.createStream(aScope, aStreamName, staticStreamConfig(1))

            txUUID <- PravegaStream.writeTx(aStreamName, personStreamWriterSettings)
            txSink  = PravegaStream.sharedTransactionalSink(aStreamName, txUUID, personStreamWriterSettings, false)
            _ <- testStream(0, 50)
                   .run(txSink)

            txSink2 = PravegaStream.sharedTransactionalSink(aStreamName, txUUID, personStreamWriterSettings, true)
            _ <- testStream(0, 50)
                   .tap(p => ZIO.when(p.age.equals(25))(ZIO.die(FakeException("Boom"))))
                   .run(txSink2)
                   .sandbox
                   .ignore
            _     <- createGroup(aGroupName, aStreamName)
            stream = PravegaStream.stream(aGroupName, personReaderSettings)
            count <- stream.take(1).runCount.timeout(1.seconds)

          } yield assert(count)(equalTo(None))
        }
      ) @@ withLiveClock
    )
  )

}
