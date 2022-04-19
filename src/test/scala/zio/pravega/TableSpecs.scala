package zio.pravega

import zio._
import zio.stream._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._
import io.pravega.client.stream.impl.UTF8StringSerializer
import io.pravega.client.tables.KeyValueTableClientConfiguration

trait TableSpecs { this: ZIOSpec[_] =>

  private def testStream(a: Int, b: Int): ZStream[Any, Nothing, String] =
    Stream.fromIterable(a until b).map(i => f"$i%04d ZIO Message")

  val tableSettings = TableWriterSettingsBuilder(
    new UTF8StringSerializer,
    new UTF8StringSerializer
  )
    .build()

  val tableReaderSettings = TableReaderSettingsBuilder(
    new UTF8StringSerializer,
    new UTF8StringSerializer
  )
    .build()

  val kvtClientConfig: KeyValueTableClientConfiguration =
    KeyValueTableClientConfiguration.builder().build()

  def tableSuite(pravegaTableName: String) = {

    def writeToTable: ZIO[Scope & PravegaTableService, Throwable, Boolean] =
      for {
        sink <- PravegaTable(
          _.sink(pravegaTableName, tableSettings, kvtClientConfig)
        )
        _ <- testStream(0, 1000)
          .map(str => (str.substring(0, 4), str))
          .run(sink)

      } yield true

    def readFromTable: ZIO[Scope & PravegaTableService, Throwable, Int] = for {
      source <- PravegaTable(
        _.source(pravegaTableName, tableReaderSettings, kvtClientConfig)
      )
      count <- source
        .take(1000)
        // .tap(m => ZIO.debug(m))
        .runFold(0)((s, _) => s + 1)

    } yield count

    suite("Tables")(
      test(s"Write to table $pravegaTableName")(
        writeToTable.map(res => assertTrue(res))
      ),
      test(s"Read from table $pravegaTableName")(
        readFromTable.map(res => assert(res)(equalTo(1000)))
      )
    ) @@ sequential
  }
}
