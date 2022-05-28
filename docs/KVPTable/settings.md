---
sidebar_position: 1
---



```scala mdoc:invisible
import zio.pravega._
import io.pravega.client.stream.impl.UTF8StringSerializer
import io.pravega.client.stream.Serializer
import java.nio.ByteBuffer

val intSerializer = new Serializer[Int] {
    override def serialize(value: Int): ByteBuffer = {
      val buff = ByteBuffer.allocate(4).putInt(value)
      buff.position(0)
      buff
    }

    override def deserialize(serializedValue: ByteBuffer): Int =
      serializedValue.getInt
  }

```
# Settings

## Table Writter settings

```scala mdoc:silent

val tableWriterSettings = TableWriterSettingsBuilder(
    new UTF8StringSerializer,
    intSerializer
  ).build()

  
```

## Table Reader settings

```scala mdoc:silent

val tableReaderSettings = TableReaderSettingsBuilder(
    new UTF8StringSerializer,
    intSerializer
  ).build()
```