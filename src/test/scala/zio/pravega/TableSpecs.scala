package zio.pravega

import zio._
import zio.stream._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._

object TableSpecs extends SharedPravegaContainerSpec("table") {

  override def spec: Spec[Environment with TestEnvironment, Any] =
    scopedSuite(
      suite("Tables")(
        test("Create table") {
          table("ages")
            .map(_ => assertCompletes)
        },
        tableSuite("ages")
      )
    )

  import CommonTestSettings._

  private def stringTestStream(
      a: Int,
      b: Int
  ): ZStream[Any, Nothing, (String, Int)] =
    ZStream.fromIterable(a until b).map(i => (f"$i%04d", i))

  def tableSuite(pravegaTableName: String) = {

    def writeToTable: ZIO[PravegaTable, Throwable, Boolean] =
      for {

        _ <- stringTestStream(0, 1000)
          .run(
            PravegaTable
              .sink(
                pravegaTableName,
                tableWriterSettings,
                (a: Int, b: Int) => a + b
              )
          )

      } yield true

    def readFromTable: ZIO[PravegaTable, Throwable, Int] =
      for {
        _ <- ZIO.logInfo("Read from table")
        source = PravegaTable
          .source(pravegaTableName, tableReaderSettings)
        count <- source
          .runFold(0)((s, _) => s + 1)
      } yield count

    def writeFlowToTable: ZIO[PravegaTable, Throwable, Int] =
      ZIO.scoped(for {
        flow <- PravegaTable
          .writerFlow(
            pravegaTableName,
            tableWriterSettings,
            (a: Int, b: Int) => a + b
          )
        count <- stringTestStream(2000, 3000)
          .via(flow)
          .runFold(0)((s, _) => s + 1)

      } yield count)

    def flowFromTable: ZIO[PravegaTable, Throwable, Int] =
      ZIO.scoped(for {
        flow <- PravegaTable
          .readerFlow(pravegaTableName, tableReaderSettings)

        count <- stringTestStream(0, 1001)
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
