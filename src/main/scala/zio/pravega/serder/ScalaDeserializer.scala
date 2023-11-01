package zio.pravega.serder

trait ScalaDeserializer[T] {
  def deserialize(bytes: Array[Byte]): T
}
