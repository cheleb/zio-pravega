package zio.pravega

import zio._
import zio.stream._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._

trait TableSpecs {
  this: ZIOSpec[
    PravegaStreamService & PravegaAdmin & PravegaTableService
  ] =>

  import CommonSettings._

  private def testStream(a: Int, b: Int): ZStream[Any, Nothing, (String, Int)] =
    ZStream.fromIterable(a until b).map(i => (f"$i%04d", i))

  def tableSuite(pravegaTableName: String) = {

    def writeToTable: ZIO[PravegaTableService, Throwable, Boolean] =
      ZIO.scoped(for {
        sink <- PravegaTable
          .sink(
            pravegaTableName,
            tableWriterSettings,
            (a: Int, b: Int) => a + b
          )

        _ <- testStream(0, 1000)
          .run(sink)

      } yield true)

    def readFromTable: ZIO[PravegaTableService, Throwable, Int] =
      ZIO.scoped(for {
        source <- PravegaTable
          .source(pravegaTableName, tableReaderSettings)
        count <- source
          .runFold(0)((s, _) => s + 1)
        // _ <- source.runFold(0)((s, _) => s + 1)
      } yield count)

    def writeFlowToTable: ZIO[PravegaTableService, Throwable, Int] =
      ZIO.scoped(for {
        flow <- PravegaTable
          .flow(
            pravegaTableName,
            tableWriterSettings,
            (a: Int, b: Int) => a + b
          )
        count <- testStream(2000, 3000)
          .via(flow)
          .runFold(0)((s, _) => s + 1)

      } yield count)

    def flowFromTable: ZIO[PravegaTableService, Throwable, Int] =
      ZIO.scoped(for {
        flow <- PravegaTable
          .flow(pravegaTableName, tableReaderSettings)

        count <- testStream(0, 1001)
          .map(_._1)
          .via(flow)
          .collect { case Some(str) => str }
          .runFold(0)((s, _) => s + 1)

      } yield count)

    suite("Tables")(
      test(s"Write to table $pravegaTableName")(
        writeToTable.map(res => assertTrue(res))
      ),
      test(s"Write through flow $pravegaTableName")(
        writeFlowToTable.map(res => assert(res)(equalTo(1000)))
      ),
      test(s"Read from table $pravegaTableName")(
        readFromTable.map(res => assert(res)(equalTo(2000)))
      ),
      test("Read through flow")(
        flowFromTable.map(res => assert(res)(equalTo(1000)))
      )
    ) @@ sequential
  }
}
