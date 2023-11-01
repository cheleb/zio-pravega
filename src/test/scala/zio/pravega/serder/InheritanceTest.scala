package zio.pravega.serder

import zio.pravega.ReaderSettingsBuilder

import zio.pravega.PravegaStream

import zio.pravega.WriterSettingsBuilder
import io.pravega.client.stream.Serializer
import java.nio.ByteBuffer

sealed trait ParentClass

final case class ChildClass1(a: Int, b: String) extends ParentClass
final case class ChildClass2(a: Int, b: String) extends ParentClass
final case class ChildClass3(a: Int, b: String) extends ParentClass

class InheritanceTest extends munit.FunSuite {
  test("Reader settings should be able to deserialize child classes") {

    val readerSettings = ReaderSettingsBuilder()
      .withDeserializer(new ScalaDeserializer[ParentClass] {

        override def deserialize(bytes: Array[Byte]): ParentClass = ???

      })

    val writterSettings = WriterSettingsBuilder()
      .withSerializer(new Serializer[ParentClass] {

        override def serialize(value: ParentClass): ByteBuffer = ???

        override def deserialize(serializedValue: ByteBuffer): ParentClass = ???

      })

    val stream = PravegaStream.stream("a-reader-group", readerSettings)
    val sink   = PravegaStream.sink("a-stream", writterSettings)

  }
}
