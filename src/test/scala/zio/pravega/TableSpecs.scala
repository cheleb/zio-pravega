package zio.pravega

import zio._
import zio.stream._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._

object TableSpecs extends SharedPravegaContainerSpec("table") {

  override def spec: Spec[Environment with TestEnvironment, Any] =
    scopedSuite(suite("Tables")(test("Create table")(table("ages").map(_ => assertCompletes)), tableSuite("ages")))

  import CommonTestSettings._

  private def keyTestStream(a: Int, b: Int): ZStream[Any, Nothing, String] = ZStream
    .fromIterable(a until b)
    .map(i => f"$i%04d")

  private def keyValueTestStream(a: Int, b: Int): ZStream[Any, Nothing, (String, Int)] = ZStream
    .fromIterable(a until b)
    .map(i => (f"$i%04d", i))

  def tableSuite(pravegaTableName: String) = {

    def writeToTable: ZIO[PravegaTable, Throwable, Unit] =
      keyValueTestStream(0, 1000) >>>
        PravegaTable.sink(pravegaTableName, tableWriterSettings, (a: Int, b: Int) => a + b)

    def readFromTable: ZIO[PravegaTable, Throwable, Long] = for {
      _     <- ZIO.logInfo("Read from table")
      source = PravegaTable.source(pravegaTableName, tableReaderSettings)
      count <- source.runCount
    } yield count

    def writeFlowToTable: ZIO[PravegaTable, Throwable, Long] = for {
      _     <- ZIO.logInfo("Write flow to table")
      flow   = PravegaTable.writerFlow(pravegaTableName, tableWriterSettings, (a: Int, b: Int) => a + b)
      count <- keyValueTestStream(1000, 2000).via(flow).runCount

    } yield count

    def flowFromTable: ZIO[PravegaTable, Throwable, Long] = for {
      _   <- ZIO.logInfo("Flow from table")
      flow = PravegaTable.readerFlow(pravegaTableName, tableReaderSettings)

      count <- keyTestStream(0, 2001).via(flow).collect { case Some(str) => str }.runCount

    } yield count

    suite("Tables")(
      test(s"Write to table $pravegaTableName")(writeToTable.map(_ => assertCompletes)),
      test(s"Write through flow $pravegaTableName")(writeFlowToTable.map(res => assert(res)(equalTo(1000L)))),
      test(s"Read from table $pravegaTableName")(readFromTable.map(res => assert(res)(equalTo(2000L)))),
      test("Read through flow")(flowFromTable.map(res => assert(res)(equalTo(2000L))))
    ) @@ sequential
  }
}
