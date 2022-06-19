package zio.pravega

import zio._
import zio.stream._
import zio.test._
import zio.test.Assertion._
import io.pravega.client.stream.Serializer
import model.Person
import java.nio.ByteBuffer

trait StreamSpec {
  this: ZIOSpec[
    PravegaStreamService & PravegaAdminService & PravegaTableService
  ] =>

  private val personSerializer = new Serializer[Person] {

    override def serialize(person: Person): ByteBuffer =
      ByteBuffer.wrap(person.toByteArray)

    override def deserialize(buffer: ByteBuffer): Person =
      Person.parseFrom(buffer.array())

  }

  val personStremWritterSettings =
    WriterSettingsBuilder()
      .withSerializer(personSerializer)

  val personStremWritterSettingsWithKey =
    WriterSettingsBuilder[Person]()
      .withKeyExtractor(_.key)
      .withSerializer(personSerializer)

  val n = 10

  import CommonSettings._

  private def testStream(a: Int, b: Int): ZStream[Any, Nothing, Person] =
    ZStream
      .fromIterable(a until b)
      .map(i => Person(key = f"$i%04d", name = s"name $i", age = i % 111))

  def streamSuite(
      pravegaStreamName: String,
      groupName: String
  ) = {

    val writeToAndConsumeStream = ZIO.scoped {
      for {
        sink <- PravegaStream.sink(
          pravegaStreamName,
          personStremWritterSettings
        )
        sinkKey <- PravegaStream.sink(
          pravegaStreamName,
          personStremWritterSettingsWithKey
        )
        _ <- testStream(0, 10).run(sink)
        _ <- (ZIO.attemptBlocking(Thread.sleep(2000)) *> ZIO.logDebug(
          "(( Re-start producing ))"
        ) *> testStream(10, 20).run(sinkKey)).fork

        stream1 <- PravegaStream.stream(groupName, readerSettings)
        stream2 <- PravegaStream.stream(groupName, readerSettings)

        _ <- ZIO.logDebug("Consuming...")
        count1Fiber <- stream1
          .take(n.toLong)
          .tap(m => ZIO.logDebug(m))
          .runFold(0)((s, _) => s + 1)
          .fork

        count2Fiber <- stream2
          .take(n.toLong)
          .tap(m => ZIO.logDebug(m))
          .runFold(0)((s, _) => s + 1)
          .fork

        count1 <- count1Fiber.join
        count2 <- count2Fiber.join

      } yield count1 + count2
    }

    suite("Stream")(
      test("publish and consume")(
        writeToAndConsumeStream
          .map(count => assert(count)(equalTo(20)))
      )
    )
  }
}
