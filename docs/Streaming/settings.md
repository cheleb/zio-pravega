---
sidebar_position: 1
---

```scala mdoc:invisible
import zio._
import zio.pravega._
import io.pravega.client.stream.impl.UTF8StringSerializer

import scala.language.postfixOps

```
# Settings

## Writter settings

```scala mdoc:silent

val writerSettings =
    WriterSettingsBuilder()
      .eventWriterConfigBuilder(_.enableLargeEvents(true))
      .withSerializer(new UTF8StringSerializer)

```

## Reader settings

```scala mdoc:silent

val readerSettings =
    ReaderSettingsBuilder()
      .withTimeout(10 seconds)
      .withSerializer(new UTF8StringSerializer)

```