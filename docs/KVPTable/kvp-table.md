---
sidebar_position: 3
---

```scala mdoc:invisible
//import zio.Console._
import zio._
import zio.stream._
import zio.pravega._
import io.pravega.client.stream.Serializer
import io.pravega.client.stream.impl.UTF8StringSerializer
import java.nio.ByteBuffer


```

With a int serializer:

```scala mdoc:silent
 val intSerializer = new Serializer[Int] {
    override def serialize(value: Int): ByteBuffer = {
      val buff = ByteBuffer.allocate(4).putInt(value)
      buff.position(0)
      buff
    }

    override def deserialize(serializedValue: ByteBuffer): Int =
      serializedValue.getInt
  }

  val tableWriterSettings = TableWriterSettingsBuilder(
    new UTF8StringSerializer,
    intSerializer
  )
    .build()

  val tableReaderSettings = TableReaderSettingsBuilder(
    new UTF8StringSerializer,
    intSerializer
  )
    .build()
```

## Producer

To write in a stream.

Given a Key-Value ZStream:
```scala mdoc:silent
private def testStream(a: Int, b: Int): ZStream[Any, Nothing, (String, Int)] =
    ZStream.fromIterable(a until b).map(i => (f"$i%04d", i))
```

Just allocate a (K, V) sink ... et voilà.

```scala mdoc:silent
def writeToTable: ZIO[PravegaTable, Throwable, Boolean] =
      ZIO.scoped(for {
        sink <- PravegaTable
          .sink("tableName", tableWriterSettings, (a: Int, b: Int) => a + b)

        _ <- testStream(0, 1000)
          .run(sink)

      } yield true)
``` 
## Consumer


