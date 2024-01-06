package zio.pravega

import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.ZIO

object StreamAndTableSpec extends SharedPravegaContainerSpec("stream-and-table") {

  import CommonTestSettings._

  override def spec: Spec[Environment with TestEnvironment, Any] = scopedSuite(
    suite("Table concurency writes")(
      test("Write concurently from stream to table")(
        stream2table.map(_ => assertCompletes)
      ),
      test("Sum table") {
        for {
          _     <- ZIO.logDebug("Sum table")
          source = PravegaTable.source("ages", tableReaderSettings)
          sum   <- source.runFold(0)((s, i) => s + i.value)
        } yield assert(sum)(equalTo(1560))
      }
    ) @@ sequential
  )

  private def stream2table = for {
    _ <- table("ages")
    _ <- createStream("persons")
    _ <- createGroup("g1", "persons")

    sink0 = sink("persons")

    stream = PravegaStream.stream("g1", personReaderSettings)

    tableSink = PravegaTable.sink("ages", tableWriterSettings, (a: Int, b: Int) => a + b)

    _ <- testStream(0, 40).run(sink0).fork

    count <- stream
               .take(40)
               .map(str => (str.key, str.age))
               .broadcast(2, 1)
               .flatMap(streams =>
                 for {
                   sink0 <- (streams(0) >>> tableSink).fork
                   sink1 <- (streams(1) >>> tableSink).fork
                   _     <- sink0.join.zipPar(sink1.join)
                 } yield ()
               )

  } yield count

}
