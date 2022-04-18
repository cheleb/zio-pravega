package zio.pravega

import zio.test.Assertion._
import zio.pravega.WriterSettingsBuilder
import io.pravega.client.stream.impl.UTF8StringSerializer
import zio.pravega.ReaderSettingsBuilder

import zio.stream._

import zio._
import zio.test._
import zio.test.TestAspect._
import zio.test.TestClock._

import io.pravega.client.stream.StreamConfiguration
import io.pravega.client.stream.ScalingPolicy
import zio.stream.ZStream
import zio.pravega._

import zio.pravega.test.PravegaContainer
import io.pravega.client.tables.KeyValueTableConfiguration

import io.pravega.client.tables.KeyValueTableClientConfiguration

object PravegaITs
    extends ZIOSpec[
      PravegaStreamService & PravegaAdminService & PravegaTableService
    ] {

  val pravegaScope = "zio-scope"
  val pravegaStreamName = "zio-stream"
  val pravegaTableName = "ziotable"

  val layer: ZLayer[Scope, TestFailure[
    Nothing
  ], PravegaAdmin & PravegaStreamService & PravegaTableService] =
    PravegaContainer.pravega >>> PravegaContainer.clientConfig >>> (PravegaAdmin.layer ++
      PravegaStreamLayer
        .fromScope(pravegaScope)
        .mapError(t => TestFailure.die(t)) ++
      PravegaTableLayer
        .fromScope(pravegaScope)
        .mapError(t => TestFailure.die(t)))

  val groupName = "coco1"

  val n = 10

  val writterSettings =
    WriterSettingsBuilder()
      .withSerializer(new UTF8StringSerializer)

  val readerSettings =
    ReaderSettingsBuilder()
      .withSerializer(new UTF8StringSerializer)

  val clientConfig = writterSettings.clientConfig

  val tableConfig = KeyValueTableConfiguration
    .builder()
    .partitionCount(2)
    .primaryKeyLength(4)
    .build()

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

  def testStream(a: Int, b: Int): ZStream[Any, Nothing, String] =
    Stream.fromIterable(a until b).map(i => f"$i%04d ZIO Message")

  private val writeToAndConsumeStream: ZIO[
    Scope & PravegaStreamService & PravegaAdminService,
    Throwable,
    Int
  ] =
    for {
      sink <- PravegaStream(_.sink(pravegaStreamName, writterSettings))
      _ <- testStream(0, 10).run(sink)
      _ <- (ZIO.sleep(2.seconds) *> ZIO.logDebug(
        "(( Re-start producing ))"
      ) *> testStream(10, 20).run(sink)).fork

      _ <- PravegaAdminService(
        _.readerGroup(
          pravegaScope,
          groupName,
          pravegaStreamName
        )
      )
      stream <- PravegaStream(_.stream(groupName, readerSettings))
      _ <- ZIO.logDebug("Consuming...")
      count <- stream
        .take(n.toLong * 2)
        .tap(e =>
          adjust(200.millis) *>
            ZIO.logDebug(s"ZStream of [$e]")
        )
        .runFold(0)((s, _) => s + 1)

      _ <- ZIO.logDebug(s"Consumed $count messages")

    } yield count

  def writeToTable: ZIO[Scope & PravegaTableService, Throwable, Boolean] = for {
    sink <- PravegaTable(
      _.sink(pravegaTableName, tableSettings, kvtClientConfig)
    )
    _ <- testStream(0, 1000).map(str => (str.substring(0, 4), str)).run(sink)

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

  def spec =
    suite("Pravega")(
      test("Scope created once")(
        PravegaAdminService(_.createScope(pravegaScope))
          .map(once => assert(once)(isTrue))
      ),
      test("Scope skip twice")(
        PravegaAdminService(_.createScope(pravegaScope))
          .map(twice => assert(twice)(isFalse))
      ),
      test("Stream created once")(
        PravegaAdminService(
          _.createStream(
            pravegaStreamName,
            StreamConfiguration.builder
              .scalingPolicy(ScalingPolicy.fixed(8))
              .build,
            pravegaScope
          )
        )
          .map(once => assert(once)(isTrue))
      ),
      test("Stream creation skiped")(
        PravegaAdminService(
          _.createStream(
            pravegaStreamName,
            StreamConfiguration.builder
              .scalingPolicy(ScalingPolicy.fixed(8))
              .build,
            pravegaScope
          )
        )
          .map(twice => assert(twice)(isFalse))
      ),
      test("publish and consume")(
        writeToAndConsumeStream
          .map(count => assert(count)(equalTo(20)))
      ),
      test(s"Create table $pravegaTableName")(
        PravegaAdminService(
          _.createTable(
            pravegaTableName,
            tableConfig,
            pravegaScope
          )
        )
          .map(created => assert(created)(isTrue))
      ),
      test(s"Write to table $pravegaTableName")(
        writeToTable.map(res => assertTrue(res))
      ),
      test(s"Read from table $pravegaTableName")(
        readFromTable.map(res => assert(res)(equalTo(1000)))
      )
    ) @@ sequential

}
