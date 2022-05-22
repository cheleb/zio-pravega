package zio.pravega

import zio._
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._
import io.pravega.client.tables.KeyValueTableConfiguration
import io.pravega.client.stream.Serializer
import model.Person
import java.nio.ByteBuffer

trait StreamAndTableSpec {
  this: ZIOSpec[
    PravegaAdminService & PravegaStreamService & PravegaTableService
  ] =>

  private val personSerializer = new Serializer[Person] {

    override def serialize(person: Person): ByteBuffer =
      ByteBuffer.wrap(person.toByteArray)

    override def deserialize(buffer: ByteBuffer): Person =
      Person.parseFrom(buffer.array())

  }
  val personReaderSettings =
    ReaderSettingsBuilder()
      .withSerializer(personSerializer)

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
      personReaderSettings
    )
    table <- PravegaTableService.sink(
      tableName,
      CommonSettings.tableWriterSettings,
      (a: Int, b: Int) => a + b
    )

    count <- stream
      .take(20)
      .map(str => (str.key, str.age))
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
