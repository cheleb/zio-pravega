package zio.pravega

import zio._
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._

import zio.pravega.test.PravegaContainer

import zio.{Scope, ZLayer}
import io.pravega.client.stream.StreamConfiguration
import io.pravega.client.stream.ScalingPolicy
import zio.test.Spec
import zio.logging.backend.SLF4J
import io.pravega.client.stream.Serializer
import model.Person
import java.nio.ByteBuffer
import zio.stream.ZStream

import scala.language.postfixOps
import io.pravega.client.tables.KeyValueTableConfiguration

abstract class SharedPravegaContainerSpec(val aScope: String)
    extends ZIOSpec[PravegaContainer] {

  val logger = zio.Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  val clientConfig = PravegaClientConfig.default

  def streamConfig(partitions: Int) = StreamConfiguration.builder
    .scalingPolicy(ScalingPolicy.fixed(partitions))
    .build

  private val tableConfig = KeyValueTableConfiguration
    .builder()
    .partitionCount(2)
    .primaryKeyLength(4)
    .build()

  /** Create a scope and provide it to the test.
    */
  def scopedSuite(
      aSuite: Spec[
        PravegaAdmin with PravegaStreamService with PravegaTableService with Scope,
        Throwable
      ]
  ): Spec[Any, Throwable] =
    suite(s"Within $aScope")(
      test(s"Created scope")(
        PravegaAdmin
          .createScope(aScope)
          .map(once => assert(once)(isTrue))
      ),
      aSuite
    ).provide(
      Scope.default,
      logger,
      PravegaAdmin.live(clientConfig),
      PravegaStream.fromScope(aScope, clientConfig),
      PravegaTable.fromScope(aScope, clientConfig)
    ) @@ sequential

  override def bootstrap: ZLayer[Scope, Any, PravegaContainer] =
    PravegaContainer.pravega

    def stream(name: String, partition: Int = 2) =
      PravegaAdmin.createStream(aScope, name, streamConfig(partition))
    def table(name: String) =
      PravegaAdmin.createTable(aScope, name, tableConfig)

  def group(name: String, stream: String) =
    PravegaAdmin
      .createReaderGroup(
        aScope,
        name,
        stream
      )

  def sink(streamName: String, routingKey: Boolean = false) =
    PravegaStream.sink(
      streamName,
      if (routingKey) personStremWritterSettings
      else personStremWritterSettingsWithKey
    )

  def sinkTx(streamName: String, routingKey: Boolean = false) =
    PravegaStream.sinkTx(
      streamName,
      if (routingKey) personStremWritterSettings
      else personStremWritterSettingsWithKey
    )

  protected val personSerializer = new Serializer[Person] {

    override def serialize(person: Person): ByteBuffer =
      ByteBuffer.wrap(person.toByteArray)

    override def deserialize(buffer: ByteBuffer): Person =
      Person.parseFrom(buffer.array())

  }

  val personReaderSettings =
    ReaderSettingsBuilder()
      .withTimeout(2 seconds)
      .withSerializer(personSerializer)

  protected val personStremWritterSettings =
    WriterSettingsBuilder()
      .withSerializer(personSerializer)

  protected val personStremWritterSettingsWithKey =
    WriterSettingsBuilder[Person]()
      .withKeyExtractor(_.key)
      .withSerializer(personSerializer)

  protected def testStream(a: Int, b: Int): ZStream[Any, Nothing, Person] =
    ZStream
      .fromIterable(a until b)
      .map(i => Person(key = f"$i%04d", name = s"name $i", age = i % 111))

}
