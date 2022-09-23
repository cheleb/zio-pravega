package zio.pravega

import zio._
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._

object StreamAndTableSpec
    extends SharedPravegaContainerSpec("stream-and-table") {

  import CommonTestSettings._

  override def spec: Spec[Environment with TestEnvironment with Scope, Any] =
    scopedSuite(
      suite("Table concurency writes")(
        test("Write concurently from stream to table") {
          stream2table.map(count => assert(count)(equalTo(80)))
        },
        test("Sum table") {
          for {
            source <- PravegaTable.source(
              "ages",
              tableReaderSettings
            )
            sum <- source.runFold(0)((s, i) => s + i.value)
          } yield assert(sum)(equalTo(1560))
        }
      ) @@ sequential
    )

  def stream2table = for {
    _ <- table("ages")
    _ <- stream("persons")
    _ <- group("g1", "persons")

    sink <- sink("persons")

    stream <- PravegaStream.stream(
      "g1",
      personReaderSettings
    )

    tableSink <- PravegaTable.sink(
      "ages",
      tableWriterSettings,
      (a: Int, b: Int) => a + b
    )

    _ <- testStream(0, 40).run(sink).fork

    count <- stream
      .take(40)
      .map(str => (str.key, str.age))
      .broadcast(2, 1)
      .flatMap(streams =>
        for {
          sink0 <-
            streams(0)
              .tapSink(tableSink)
              .runFold(0)((s, _) => s + 1)
              .fork
          sink1 <-
            streams(1)
              .tapSink(tableSink)
              .runFold(0)((s, _) => s + 1)
              .fork
          x <- sink0.join.zipPar(sink1.join)
        } yield x._1 + x._2
      )

  } yield count

}
