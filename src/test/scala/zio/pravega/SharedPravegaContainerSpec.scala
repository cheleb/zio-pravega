package zio.pravega

import zio.test._

import zio.test.TestAspect._

import zio.pravega.test.PravegaContainer

import zio.{Scope, ZLayer}
import io.pravega.client.stream.StreamConfiguration
import io.pravega.client.stream.ScalingPolicy
import zio.test.Spec

import model.Person

import zio.stream.ZStream
import zio.pravega.admin._
import io.pravega.client.tables.KeyValueTableConfiguration

abstract class SharedPravegaContainerSpec(val aScope: String) extends ZIOSpec[PravegaContainer] {

  import CommonTestSettings._

  val clientConfig = PravegaClientConfig.default

  def dynamicStreamConfig(targetRate: Int, scaleFactor: Int, minNumSegments: Int) = StreamConfiguration.builder
    .scalingPolicy(ScalingPolicy.byEventRate(targetRate, scaleFactor, minNumSegments))
    .build

  def staticStreamConfig(partitions: Int) =
    StreamConfiguration.builder.scalingPolicy(ScalingPolicy.fixed(partitions)).build

  private val tableConfig = KeyValueTableConfiguration.builder().partitionCount(2).primaryKeyLength(4).build()

  /**
   * Create a scope and provide it to the test.
   */
  def scopedSuite(
    aSuite: Spec[
      PravegaStreamManager with PravegaReaderGroupManager with PravegaTableManager with PravegaStream with PravegaTable with Scope,
      Throwable
    ]
  ): Spec[Any, Throwable] =
    suite(s"Within $aScope")(
      test(s"Created scope")(PravegaStreamManager.createScope(aScope).map(_ => assertCompletes)),
      aSuite
    )
      .provide(
        Scope.default,
        PravegaClientConfig.live,
        PravegaStreamManager.live,
        PravegaReaderGroupManager.live(aScope),
        PravegaTableManager.live,
        PravegaStream.fromScope(aScope),
        PravegaTable.fromScope(aScope)
      ) @@ sequential

  override def bootstrap: ZLayer[Any, Nothing, PravegaContainer] = PravegaContainer.pravega

  def createStream(name: String, partition: Int = 2) = PravegaStreamManager
    .createStream(aScope, name, staticStreamConfig(partition))
  def table(name: String) = PravegaTableManager.createTable(aScope, name, tableConfig)

  def createGroup(name: String, stream: String) = PravegaReaderGroupManager.createReaderGroup(name, stream)

  def sink(streamName: String, routingKey: Boolean = false) = PravegaStream
    .sink(streamName, if (routingKey) personStremWritterSettings else personStremWritterSettingsWithKey)

  def sinkTx(streamName: String, routingKey: Boolean = false) = PravegaStream
    .sinkTx(streamName, if (routingKey) personStremWritterSettings else personStremWritterSettingsWithKey)

  protected def testStream(a: Int, b: Int): ZStream[Any, Nothing, Person] = ZStream
    .fromIterable(a until b)
    .map(i => Person(key = f"$i%04d", name = s"name $i", age = i % 111))

}
