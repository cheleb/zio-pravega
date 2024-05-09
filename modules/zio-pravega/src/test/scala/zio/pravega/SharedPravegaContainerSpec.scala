package zio.pravega

import zio.test._

import zio.test.TestAspect._

import zio.pravega.test.PravegaContainer

import zio._
import io.pravega.client.stream.StreamConfiguration
import io.pravega.client.stream.ScalingPolicy
import zio.test.Spec

import model.Person

import zio.stream.ZStream
import zio.pravega.admin._
import io.pravega.client.tables.KeyValueTableConfiguration
import io.pravega.client.ClientConfig
import zio.stream.ZSink

abstract class SharedPravegaContainerSpec(val aScope: String) extends ZIOSpec[PravegaContainer] {

  import CommonTestSettings._

  val clientConfig: ClientConfig = PravegaClientConfig.default

  def dynamicStreamConfig(targetRate: Int, scaleFactor: Int, minNumSegments: Int): StreamConfiguration =
    StreamConfiguration.builder
      .scalingPolicy(ScalingPolicy.byEventRate(targetRate, scaleFactor, minNumSegments))
      .build

  def staticStreamConfig(partitions: Int): StreamConfiguration =
    StreamConfiguration.builder.scalingPolicy(ScalingPolicy.fixed(partitions)).build

  private val tableConfig = KeyValueTableConfiguration.builder().partitionCount(2).primaryKeyLength(4).build()

  /**
   * Create a scope and provide it to the test.
   */
  def scopedSuite(
    aSuite: Spec[
      PravegaStreamManager & PravegaReaderGroupManager & PravegaTableManager & PravegaStream & PravegaTable & Scope,
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

  def createStream(name: String, partition: Int = 2): RIO[PravegaStreamManager, Boolean] = PravegaStreamManager
    .createStream(aScope, name, staticStreamConfig(partition))
  def table(name: String): RIO[PravegaTableManager & Scope, Boolean] =
    PravegaTableManager.createTable(aScope, name, tableConfig)

  def createGroup(name: String, stream: String): ZIO[PravegaReaderGroupManager, Throwable, Boolean] =
    PravegaReaderGroupManager.createReaderGroup(name, stream)

  def source(groupName: String): ZStream[PravegaStream, Throwable, Person] =
    PravegaStream.stream(groupName, personReaderSettings)

  def sink(streamName: String, routingKey: Boolean = false): ZSink[PravegaStream, Throwable, Person, Nothing, Unit] =
    PravegaStream
      .sink(streamName, if (routingKey) personStreamWriterSettings else personStreamWriterSettingsWithKey)

  def sinkTx(streamName: String, routingKey: Boolean = false): ZSink[PravegaStream, Throwable, Person, Nothing, Unit] =
    PravegaStream
      .transactionalSink(streamName, if (routingKey) personStreamWriterSettings else personStreamWriterSettingsWithKey)

  protected def personsStream(a: Int, b: Int): ZStream[Any, Nothing, Person] = ZStream
    .fromIterable(a until b)
    .map(i => Person(key = f"$i%04d", name = f"name $i%d", age = i % 111))

  def assertStreamCount[A](aStreamName: String)(
    zioA: (String, String) => RIO[PravegaStream, A]
  )(assertion: Assertion[A]) =
    test(aStreamName.capitalize.replaceAll("-", " ")) {
      val aGroupName = s"$aStreamName-group"
      for {
        stream <- PravegaStreamManager.createStream(aScope, aStreamName, staticStreamConfig(1))
        _      <- createGroup(aGroupName, aStreamName)

        count <- zioA(aStreamName, aGroupName)

      } yield assert(count)(assertion)
    }

}
