# Pravega settings

Pravega setting are a set of configuration parameters that are used to connect to Pravega. They all have default values in [reference.conf](https://github.com/cheleb/zio-pravega/blob/master/src/main/resources/reference.conf) and can be overriden in a programatic way or in a configuration file, using [HOCON](https://lightbend.github.io/config/) format.

## Client config.

Client config is used to connect to Pravega controller. It is used by all Pravega clients.

```scala mdoc:silent
import zio.pravega._
import java.net.URI

val clientConfig =  PravegaClientConfig.builder
    .controllerURI(new URI("tcp://localhost:9090:"))
    .enableTlsToController(true)
    .build()
```

## Serializer

Serializer is used to serialize and deserialize data to/from Pravega streams and tables.

Pravega [serializer](https://cncf.pravega.io/docs/latest/javadoc/clients/io/pravega/client/stream/Serializer.html) convert events to bytebuffer and vice versa.

### Basic serializer

Pravega provides a set of basic serializers for common types.

```scala mdoc
import io.pravega.client.stream.impl.UTF8StringSerializer

val serializer = new UTF8StringSerializer()
```

### Protobuf 

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

### ZIO Schema

TDB



