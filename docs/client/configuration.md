---
sidebar_position: 2
---

# Client 

Pravega client configuration has many options.

```scala mdoc:invisible
import zio.pravega._
import io.pravega.client.stream.impl.UTF8StringSerializer
```

```scala mdoc

val writterSettings =
    WriterSettingsBuilder()
      .withSerializer(new UTF8StringSerializer)

```