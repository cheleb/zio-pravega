---
sidebar_position: 4
---

# Serializer

Pravega [serializer](https://cncf.pravega.io/docs/latest/javadoc/clients/io/pravega/client/stream/Serializer.html) convert events to bytebuffer and vice versa.



## Protobuf 

```scala mdoc:invisible
```
Easy to implement with protobuf.

```scala
import io.pravega.client.stream.Serializer
import model.Person
import java.nio.ByteBuffer

val personSerializer = new Serializer[Person] {

    override def serialize(person: Person): ByteBuffer =
      ByteBuffer.wrap(person.toByteArray)

    override def deserialize(buffer: ByteBuffer): Person =
      Person.parseFrom(buffer.array())

  }
```