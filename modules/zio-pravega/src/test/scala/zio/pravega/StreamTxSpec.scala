package zio.pravega

import zio._

import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.pravega.admin._

import model.Person

object StreamTxSpec extends SharedPravegaContainerSpec("streaming-tx") {

  import CommonTestSettings._

  override def spec: Spec[Environment & TestEnvironment, Any] = scopedSuite(
    suite("Tx Stream support")(
      assertStreamCount("write-uncommited") { (aStreamName, aGroupName) =>
        for {
          txUUID <- PravegaStream.writeUncommited(aStreamName, personStreamWriterSettings, Person("0001", "John", 20))
          _      <- writesPersons(sharedTxSink(aStreamName, txUUID, true), 1)
          count  <- readPersons(aGroupName, 2)
        } yield count
      }(equalTo(2L)),
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
          _ <- failingTxWritesPersons(sinkTx(aStreamName), 1, 100, 50)
          _ <- writesPersonsRange(sink(aStreamName), 50, 100)

          count <- source(aGroupName).take(50).filter(_.age < 50).runCount

        } yield count
      }(equalTo(0L)),
      suite("Distributed tx success")(
        assertStreamCount("tx-2-writers-success") { (aStreamName, aGroupName) =>
          for {
            txUUID <- PravegaStream.openTransaction(aStreamName, personStreamWriterSettings)
            _      <- writesPersons(sharedTxSink(aStreamName, txUUID))
            _      <- writesPersons(sharedTxSink(aStreamName, txUUID, true)).fork
            count  <- readPersons(aGroupName, 100)

          } yield count
        }(equalTo(100L)),
        assertStreamCount("tx-3-writers-success") { (aStreamName, aGroupName) =>
          for {

            txUUID <- PravegaStream.openTransaction(aStreamName, personStreamWriterSettings)

            _ <- writesPersons(sharedTxSink(aStreamName, txUUID))
            _ <- writesPersons(sharedTxSink(aStreamName, txUUID))
            _ <- writesPersons(sharedTxSink(aStreamName, txUUID, true))
            // Read from the stream

            count <- readPersons(aGroupName, 150)

          } yield count
        }(equalTo(150L)),
        assertStreamCount("tx-3-writers-success-closed") { (aStreamName, aGroupName) =>
          for {
            txUUID <- PravegaStream.openTransaction(aStreamName, personStreamWriterSettings)
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
          val txSink = PravegaStream.transactionalSinkUncommited(aStreamName, personStreamWriterSettings)
          for {

            _ <- failingTxWritesPersons(txSink, 0, 50, 25)

            count <- readPersons(aGroupName, 1).timeout(1.seconds)

          } yield count
        }(equalTo(None)),
        assertStreamCount("tx-second-fail") { (aStreamName, aGroupName) =>
          for {
            txUUID <- PravegaStream.openTransaction(aStreamName, personStreamWriterSettings)
            _      <- failingTxWritesPersons(sharedTxSink(aStreamName, txUUID), 0, 50, 25)

            count <- readPersons(aGroupName, 1).timeout(1.seconds)

          } yield count
        }(equalTo(None)),
        assertStreamCount("tx-2writer-ssecond-fail") { (aStreamName, aGroupName) =>
          for {
            txUUID <- PravegaStream.openTransaction(aStreamName, personStreamWriterSettings)
            _      <- writesPersons(sharedTxSink(aStreamName, txUUID))

            _     <- failingTxWritesPersons(sharedTxSink(aStreamName, txUUID, true), 0, 50, 25)
            count <- readPersons(aGroupName, 1).timeout(1.seconds)

          } yield count
        }(equalTo(None))
      ) @@ withLiveClock
    )
  )

}
