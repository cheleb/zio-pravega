# Pravega ZIO

Proof of concept to use [Pravega](https://www.pravega.io) with [ZIO](https://www.zio.dev)


# Usage

## Settings

### Admin

#### Scope and Stream

Can be manager through StreamManager:

```scala
def initScopeAndStream: ZIO[Has[Console], Throwable, Boolean] = PravegaAdmin.streamManager(clientConfig).use {
    streamManager =>
      ZIO.attemptBlocking(streamManager.createScope("zio-scope")) *> ZIO
        .attemptBlocking(
          streamManager.createStream(
            "zio-scope",
            "zio-stream",
            StreamConfiguration.builder
              .scalingPolicy(ScalingPolicy.fixed(8))
              .build
          )
        ) <* printLine("Scope and stream inited")

  }
```

### WriterSettings

[Default settings](src/main/resources/reference.conf) but you need at least to provide and event serializer.

```scala
val writterSettings =
    WriterSettingsBuilder()
      .withSerializer(new UTF8StringSerializer)
```

### ReaderSettings
[Default settings](src/main/resources/reference.conf) but you need at least to provide and event serializer.

```scala
  val readerSettings =
    ReaderSettingsBuilder()
      .withSerializer(new UTF8StringSerializer)
```

### Writing and reading from stream

Quite straigh forward:

```scala
private val writeToAndConsumeStream: ZIO[Has[Clock] with Has[Console] with Has[Service] with Has[Console],Any,Int] = for {
    // Make a sink
    sink <- pravegaSink("zio-stream", writterSettings)
    // Produce a test input stream
    testStream = Stream.fromIterable(0 until 10).map(i => s"ZIO Message $i")
    // Consume input stream to sink
    _    <- testStream(0, 10).run(sink)

    // Reader group will be created if not already existing.
    _ <- readerGroup(group, readerSettings, "zio-stream")

    // Make a source stream
    stream <- pravegaStream(group, readerSettings)
    _      <- printLine("Consuming...")
    // Consume 10 events and closes.
    count <- stream
              .take(10)
              .tap(e => printLine(s"ZStream of [$e]"))
              .fold(0)((s, _) => s + 1)
    _ <- printLine(s"Consumed $count messages")

  } yield count

```

See [full example](src/test/scala/zio/pravega/test/TestZioApp.scala).
