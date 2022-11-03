package zio.pravega

import zio._
import io.pravega.client.stream.impl.UTF8StringSerializer
import io.pravega.client.stream.Serializer
import java.nio.ByteBuffer

import scala.language.postfixOps
import model.Person

object CommonTestSettings {

  val intSerializer = new Serializer[Int] {
    override def serialize(value: Int): ByteBuffer = {
      val buff = ByteBuffer.allocate(4).putInt(value)
      buff.position(0)
      buff
    }

    override def deserialize(serializedValue: ByteBuffer): Int = serializedValue.getInt
  }

  val writterSettings = WriterSettingsBuilder()
    .eventWriterConfigBuilder(_.enableLargeEvents(true))
    .withSerializer(new UTF8StringSerializer)

  val readerSettings = ReaderSettingsBuilder().withTimeout(2 seconds).withSerializer(new UTF8StringSerializer)

  val readerSettings2 = ReaderSettingsBuilder().withTimeout(2 seconds).withSerializer(new UTF8StringSerializer)

  val tableWriterSettings = TableWriterSettingsBuilder(new UTF8StringSerializer, intSerializer).build()

  val tableReaderSettings = TableReaderSettingsBuilder(new UTF8StringSerializer, intSerializer).build()

  val personSerializer = new Serializer[Person] {

    override def serialize(person: Person): ByteBuffer = ByteBuffer.wrap(person.toByteArray)

    override def deserialize(buffer: ByteBuffer): Person = Person.parseFrom(buffer.array())

  }

  val personReaderSettings = ReaderSettingsBuilder().withTimeout(2 seconds).withSerializer(personSerializer)

  val personStremWritterSettings = WriterSettingsBuilder().withSerializer(personSerializer)

  val personStremWritterSettingsWithKey = WriterSettingsBuilder[Person]()
    .withKeyExtractor(_.key)
    .withSerializer(personSerializer)
}
