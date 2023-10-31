package zio.pravega

import serder.*

import zio._

import io.pravega.client.stream.Serializer
import java.nio.ByteBuffer

import scala.language.postfixOps
import model.Person
import io.pravega.client.stream.impl.UTF8StringSerializer

object CommonTestSettings {

  val intSerializer: Serializer[Int] = new Serializer[Int] {
    override def serialize(value: Int): ByteBuffer = {
      val buff = ByteBuffer.allocate(4).putInt(value)
      buff.position(0)
    }

    override def deserialize(serializedValue: ByteBuffer): Int = serializedValue.getInt
  }

  val writterSettings: WriterSettings[String] = WriterSettingsBuilder()
    .eventWriterConfigBuilder(_.enableLargeEvents(true))
    .withSerializer(new UTF8StringSerializer)

  val readerSettings: ReaderSettings[String] =
    ReaderSettingsBuilder().withTimeout(2 seconds).withDeserializer(UTF8StringScalaDeserializer)

  val readerSettings2: ReaderSettings[String] =
    ReaderSettingsBuilder().withTimeout(2 seconds).withDeserializer(UTF8StringScalaDeserializer)

  val tableWriterSettings: TableWriterSettings[String, Int] =
    TableWriterSettingsBuilder(new UTF8StringSerializer, intSerializer).build()

  val tableReaderSettings: TableReaderSettings[String, Int] =
    TableReaderSettingsBuilder(new UTF8StringSerializer, intSerializer).build()

  val personSerializer: Serializer[Person] = new Serializer[Person] {

    override def serialize(person: Person): ByteBuffer = ByteBuffer.wrap(person.toByteArray)

    override def deserialize(buffer: ByteBuffer): Person = Person.parseFrom(buffer.array())

  }

  val personDeserializer: ScalaDeserializer[Person] = new ScalaDeserializer[Person] {

    override def deserialize(buffer: Array[Byte]): Person = Person.parseFrom(buffer)

  }

  val personReaderSettings: ReaderSettings[Person] =
    ReaderSettingsBuilder().withTimeout(2 seconds).withDeserializer(personDeserializer)

  val personStreamWriterSettings: WriterSettings[Person] = WriterSettingsBuilder().withSerializer(personSerializer)

  val personStreamWriterSettingsWithKey: WriterSettings[Person] = WriterSettingsBuilder[Person]()
    .withKeyExtractor(_.key)
    .withSerializer(personSerializer)
}
