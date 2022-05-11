package zio.pravega

import io.pravega.client.stream.impl.UTF8StringSerializer
import io.pravega.client.stream.Serializer
import java.nio.ByteBuffer

object CommonSettings {

  val intSerializer = new Serializer[Int] {
    override def serialize(value: Int): ByteBuffer = {
      val buff = ByteBuffer.allocate(4).putInt(value)
      buff.position(0)
      buff
    }

    override def deserialize(serializedValue: ByteBuffer): Int =
      serializedValue.getInt
  }

  val writterSettings =
    WriterSettingsBuilder()
      .withSerializer(new UTF8StringSerializer)

  val readerSettings =
    ReaderSettingsBuilder()
      .withSerializer(new UTF8StringSerializer)

  val tableWriterSettings = TableWriterSettingsBuilder(
    new UTF8StringSerializer,
    intSerializer
  )((a, b) => a + b)
    .build()

  val tableReaderSettings = TableReaderSettingsBuilder(
    new UTF8StringSerializer,
    intSerializer
  )
    .build()

  val clientConfig = writterSettings.clientConfig

}
