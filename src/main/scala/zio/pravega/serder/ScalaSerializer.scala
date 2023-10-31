package zio.pravega.serder

trait ScalaSerializer[-T] {
  def serialize(t: T): Array[Byte]
}
