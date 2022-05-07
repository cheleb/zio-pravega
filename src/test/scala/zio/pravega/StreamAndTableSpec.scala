package zio.pravega

import zio._
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._
import io.pravega.client.tables.KeyValueTableConfiguration

trait StreamAndTableSpec {
  this: ZIOSpec[
    PravegaAdminService & PravegaStreamService & PravegaTableService
  ] =>

  private val tableConfig = KeyValueTableConfiguration
    .builder()
    .partitionCount(2)
    .primaryKeyLength(4)
    .build()
  val groupName = "stream2table"
  val tableName = "countTable"

  def stream2table(scope: String, streamName: String) = for {
    _ <- PravegaAdminService.createTable(tableName, tableConfig, scope)
    _ <- PravegaAdminService.readerGroup(scope, groupName, streamName)
    stream <- PravegaStreamService.stream(
      groupName,
      CommonSettings.readerSettings
    )
    // readFlow <- PravegaTable(
    //   _.flow(
    //     tableName,
    //     CommonSettings.tableReaderSettings,
    //     CommonSettings.kvtClientConfig
    //   )
    // )
    table <- PravegaTableService.sink(
      tableName,
      CommonSettings.tableWriterSettings
    )

    count <- stream
      .take(20)
//      .tap(str => ZIO.debug(str))
      .map(str => (str.substring(0, 4), str.substring(0, 4).toInt))
      .broadcast(2, 1)
      .flatMap(streams =>
        for {
          sink0 <-
            streams(0)
              .tapSink(table)
              .runFold(0)((s, _) => s + 1)
              .fork
          sink1 <-
            streams(1)
              .tapSink(table)
              .runFold(0)((s, _) => s + 1)
              .fork
          x <- sink0.join.zipPar(sink1.join)
        } yield x._1 + x._2
      )

  } yield count

  def streamAndTable(scope: String, streamName: String) =
    suite("Stream and table")(
      test("Write concurently") {
        stream2table(scope, streamName).map(count => assert(count)(equalTo(40)))
      },
      test("Sum table") {
        (for {
          source <- PravegaTableService.source(
            tableName,
            CommonSettings.tableReaderSettings
          )
          sum <- source.runFold(0)((s, i) => s + i.value)
        } yield sum).map(s => assert(s)(equalTo(380)))
      }
    ) @@ sequential

}
