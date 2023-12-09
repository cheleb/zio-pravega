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

  private def tableSuite(pravegaTableName: String) = {

    def writeToTable: ZIO[PravegaTable, Throwable, Unit] =
      keyValueTestStream(0, 1000) >>>
        PravegaTable.sink(pravegaTableName, tableWriterSettings, (a: Int, b: Int) => a + b)

    def readFromTable: ZIO[PravegaTable, Throwable, Long] = for {
      _     <- ZIO.logDebug(s"Read from table $pravegaTableName.")
      source = PravegaTable.source(pravegaTableName, tableReaderSettings)
      count <- source.runCount
      _     <- ZIO.logDebug(f"Read $count%d from table $pravegaTableName.")
    } yield count

    def writeFlowToTable: ZIO[PravegaTable, Throwable, Long] = for {
      _   <- ZIO.logDebug("Write flow to table")
      flow = PravegaTable.writerFlow(pravegaTableName, tableWriterSettings, (a: Int, b: Int) => a + b)

      count <- keyValueTestStream(1000, 2000).via(flow).runCount
    } yield count

    def flowFromTable: ZIO[PravegaTable, Throwable, Long] = for {
      _   <- ZIO.logDebug("Flow from table")
      flow = PravegaTable.readerFlow(pravegaTableName, tableReaderSettings)

      count <- keyTestStream(0, 2001).via(flow).collect { case Some(str) => str }.runCount

    } yield count

    def putToTable: ZIO[PravegaTable, Throwable, Unit] = for {
      _ <- ZIO.logDebug("Put to table")
      _ <- PravegaTable.put(pravegaTableName, "9999", 1, tableWriterSettings)
      _ <- PravegaTable.put(pravegaTableName, "9999", 2, tableWriterSettings)
    } yield ()

    def mergeinTable: ZIO[PravegaTable, Throwable, Int] = for {
      _ <- ZIO.logDebug("Put to table")
      // Type inference fails here in Scala 2.x so we need to specify the type of the combine function
      res <- PravegaTable.merge(pravegaTableName, "9999", 1, (a: Int, b: Int) => a - b, tableWriterSettings)
    } yield res

    suite("Tables")(
      test(s"Write to table $pravegaTableName")(writeToTable.map(_ => assertCompletes)),
      test(s"Write through flow $pravegaTableName")(writeFlowToTable.map(res => assert(res)(equalTo(1000L)))),
      test(s"Read from table $pravegaTableName")(readFromTable.map(res => assert(res)(equalTo(2000L)))),
      test("Read through flow")(flowFromTable.map(res => assert(res)(equalTo(2000L)))),
      test("Put to table")(putToTable.map(_ => assertCompletes)),
      test("Merge in table")(mergeinTable.map(res => assert(res)(equalTo(1))))
    ) @@ sequential
  }
}
