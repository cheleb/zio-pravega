package zio.pravega

import zio._

import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.pravega.admin._
import scala.jdk.CollectionConverters._
import model.Person

object StreamSpec extends SharedPravegaContainerSpec("streaming-timeout") {

  import CommonTestSettings._

  val writtenPersons = Seq(
    Person("1234", "John", 42),
    Person("1234", "Bill", 42),
    Person("1234", "Mary", 42)
  )

  override def spec: Spec[Environment & TestEnvironment, Any] = scopedSuite(
    suite("Stream spec")(
      test("Stream support timeouts") {
        for {
          _ <- PravegaStreamManager.createStream(aScope, "s1", staticStreamConfig(2))

          _ <- PravegaStream.write("s1", personStreamWriterSettings, Seq.empty[Person]*)
          _ <- PravegaStream.write("s1", personStreamWriterSettings, writtenPersons.head)
          _ <- PravegaStream.write("s1", personStreamWriterSettings, writtenPersons.tail*)

          _        <- PravegaReaderGroupManager.createReaderGroup("g1", "s1")
          sink1     = sink("s1")
          sink2     = sinkTx("s1")
          writeFlow = PravegaStream.writeFlow("s1", personStreamWriterSettings)
          stream1   = PravegaStream.stream("g1", personReaderSettings)
          stream2   = PravegaStream.stream("g1", personReaderSettings)

          _    <- personsStream(0, 50).run(sink1).fork
          fib1 <- stream1.take(75).runCount.fork
          fib2 <- stream2.take(75 + writtenPersons.size).runCount.fork
          _ <-
            (ZIO.sleep(2000.millis) *> ZIO.logDebug("(( Re-start producing ))") *> personsStream(50, 100).run(
              sink2
            )).fork
          fib3   <- personsStream(100, 150).via(writeFlow).runDrain.fork
          count1 <- fib1.join
          count2 <- fib2.join
          _      <- fib3.join
          count   = count1 + count2
          _      <- ZIO.logDebug(f"count $count1%d + $count2%d = $count%d")
        } yield assert(count)(equalTo(150L + writtenPersons.size))
      } @@ withLiveClock,
      test("Truncating stream") {
        for {
          readerGroup <- PravegaReaderGroupManager.openReaderGroup("g1")
          streamCuts   = readerGroup.getStreamCuts()
          _ <- ZIO.foreach(streamCuts.asScala.toList) { case (stream, streamCut) =>
                 ZIO.logDebug(s"Stream: ${stream.getStreamName}, StreamCut: $streamCut") *>
                   PravegaStreamManager.truncateStream(aScope, stream.getStreamName(), streamCut)
               }
        } yield assertCompletes
      }
    ) @@ sequential
  )
}
