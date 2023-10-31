package zio.pravega.serder

import java.nio.charset.StandardCharsets

object UTF8StringScalaSerializer extends ScalaSerializer[String] {

  def serialize(value: String): Array[Byte] = value.getBytes(StandardCharsets.UTF_8)
}

object UTF8StringScalaDeserializer extends ScalaDeserializer[String] {

  def deserialize(bytes: Array[Byte]): String = new String(bytes, StandardCharsets.UTF_8)
}
