package zio.pravega

import zio._
import zio.stream._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._

trait TableSpecs {
  this: ZIOSpec[
    PravegaStreamService & PravegaAdminService & PravegaTableService
  ] =>

  import CommonSettings._

  private def testStream(a: Int, b: Int): ZStream[Any, Nothing, String] =
    ZStream.fromIterable(a until b).map(i => f"$i%04d ZIO Message")

  def tableSuite(pravegaTableName: String) = {

    def writeToTable: ZIO[PravegaTableService, Throwable, Boolean] =
      ZIO.scoped(for {
        sink <- PravegaTableService
          .sink(pravegaTableName, tableWriterSettings, kvtClientConfig)

        _ <- testStream(0, 1000)
          .map(str => (str.substring(0, 4), str))
          .run(sink)

      } yield true)

    def readFromTable: ZIO[PravegaTableService, Throwable, Int] =
      ZIO.scoped(for {
        source <- PravegaTableService
          .source(pravegaTableName, tableReaderSettings, kvtClientConfig)
        count <- source
          .runFold(0)((s, _) => s + 1)
      } yield count)

    def writeFlowToTable: ZIO[PravegaTableService, Throwable, Int] =
      ZIO.scoped(for {
        flow <- PravegaTableService
          .flow(pravegaTableName, tableWriterSettings, kvtClientConfig)
        count <- testStream(2000, 3000)
          .map(str => (str.substring(0, 4), str))
          .via(flow)
          .runFold(0)((s, _) => s + 1)

      } yield count)

    def flowFromTable: ZIO[PravegaTableService, Throwable, Int] =
      ZIO.scoped(for {
        flow <- PravegaTableService
          .flow(pravegaTableName, tableReaderSettings, kvtClientConfig)

        count <- testStream(0, 1001)
          .map(str => str.substring(0, 4))
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
