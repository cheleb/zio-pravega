package zio.pravega

import zio._

import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.pravega.admin._

object StreamTxSpec extends SharedPravegaContainerSpec("streaming-tx") {

  import CommonTestSettings._

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  override def spec: Spec[Environment & TestEnvironment, Any] = scopedSuite(
    suite("Tx Stream support")(
      assertStreamCount("stream-support-timeout") { (aStreamName, aGroupName) =>
        for {
          _    <- writesPersons(sink(aStreamName, true)).fork
          fib1 <- readPersons(aGroupName, 50).fork
          fib2 <- source(aGroupName).take(50).runCount.fork
          _ <-
            (ZIO.sleep(2000.millis) *> ZIO.logDebug("(( Re-start producing ))") *> personsStream(50, 100).run(
              sinkTx(aStreamName, true)
            )).fork
          count1 <- fib1.join
          count2 <- fib2.join
          count   = count1 + count2
          _      <- ZIO.logDebug(f"count $count1%d + $count2%d = $count%d")
        } yield count

      }(equalTo(100L))
        @@ withLiveClock,
      assertStreamCount("roll-back-sinks") { (aStreamName, aGroupName) =>
        for {
          _ <- personsStream(1, 100)
                 .tap(p => ZIO.when(p.age.equals(50))(ZIO.die(FakeException("Boom"))))
                 .run(sinkTx(aStreamName))
                 .sandbox
                 .ignore
          _ <- writesPersonsRange(sink(aStreamName), 50, 100)

          count <- source(aGroupName).take(50).filter(_.age < 50).runCount

        } yield count
      }(equalTo(0L)),
      suite("Distributed tx success")(
        assertStreamCount("tx-2-writers-success") { (aStreamName, aGroupName) =>
          for {
            txUUID <- PravegaStream.writeTx(aStreamName, personStreamWriterSettings)
            _      <- writesPersons(sharedTxSink(aStreamName, txUUID))
            _      <- writesPersons(sharedTxSink(aStreamName, txUUID, true)).fork
            count  <- readPersons(aGroupName, 100)

          } yield count
        }(equalTo(100L)),
        assertStreamCount("tx-3-writers-success") { (aStreamName, aGroupName) =>
          for {

            txUUID <- PravegaStream.writeTx(aStreamName, personStreamWriterSettings)

            _ <- writesPersons(sharedTxSink(aStreamName, txUUID))
            _ <- writesPersons(sharedTxSink(aStreamName, txUUID))
            _ <- writesPersons(sharedTxSink(aStreamName, txUUID, true))
            // Read from the stream

            count <- readPersons(aGroupName, 150)

          } yield count
        }(equalTo(150L)),
        assertStreamCount("tx-3-writers-success-closed") { (aStreamName, aGroupName) =>
          for {
            txUUID <- PravegaStream.writeTx(aStreamName, personStreamWriterSettings)
            _      <- writesPersons(sharedTxSink(aStreamName, txUUID))
            _      <- writesPersons(sharedTxSink(aStreamName, txUUID, true))
            _      <- writesPersons(sharedTxSink(aStreamName, txUUID, true)).fork
            // Read from the stream
            count <- readPersons(aGroupName, 150).timeout(1.seconds)

          } yield count
        }(equalTo(None))
          @@ withLiveClock
      ) @@ sequential,
      suite("Distributed tx fail if one of them fails")(
        assertStreamCount("tx-first-fail") { (aStreamName, aGroupName) =>
          val txSink = PravegaStream.newSharedTransactionalSink(aStreamName, personStreamWriterSettings)
          for {

            _ <- personsStream(0, 50)
                   .tap(p => ZIO.when(p.age.equals(25))(ZIO.die(FakeException("Boom"))))
                   .run(txSink)
                   .sandbox
                   .ignore

            count <- readPersons(aGroupName, 1).timeout(1.seconds)

          } yield count
        }(equalTo(None)),
        assertStreamCount("tx-second-fail") { (aStreamName, aGroupName) =>
          for {
            txUUID <- PravegaStream.writeTx(aStreamName, personStreamWriterSettings)
            _ <- personsStream(0, 50)
                   .tap(p => ZIO.when(p.age.equals(25))(ZIO.die(FakeException("Boom"))))
                   .run(sharedTxSink(aStreamName, txUUID))
                   .sandbox
                   .ignore

            count <- readPersons(aGroupName, 1).timeout(1.seconds)

          } yield count
        }(equalTo(None)),
        assertStreamCount("tx-2writer-ssecond-fail") { (aStreamName, aGroupName) =>
          for {
            txUUID <- PravegaStream.writeTx(aStreamName, personStreamWriterSettings)
            _      <- writesPersons(sharedTxSink(aStreamName, txUUID))

            _ <- personsStream(0, 50)
                   .tap(p => ZIO.when(p.age.equals(25))(ZIO.die(FakeException("Boom"))))
                   .run(sharedTxSink(aStreamName, txUUID, true))
                   .sandbox
                   .ignore
            count <- readPersons(aGroupName, 1).timeout(1.seconds)

          } yield count
        }(equalTo(None))
      ) @@ withLiveClock
    )
  )

}
