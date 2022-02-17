# Pravega ZIO

![scala](https://github.com/cheleb/zio-pravega/actions/workflows/scala.yml/badge.svg)
[![codecov](https://codecov.io/gh/cheleb/zio-pravega/branch/master/graph/badge.svg?token=9IW44171RJ)](https://codecov.io/gh/cheleb/zio-pravega)

Proof of concept to use [Pravega](https://www.pravega.io) with [ZIO](https://www.zio.dev)

Document should land here soon [Pravega ZIO](https://cheleb.github.io/zio-pravega/)

# Usage

## Settings

### Admin

#### Scope and Stream

Can be managed through StreamManager:

```scala
def initScopeAndStream: ZIO[Has[StreamManager] with Has[Console], Throwable, Unit] =
    for {
      scopeCreated <- PravegaAdmin.createScope(scope)
      _            <- ZIO.when(scopeCreated)(printLine(s"Scope $scope just created"))
      streamCreated <- PravegaAdmin.createStream(
                        streamName,
                        StreamConfiguration.builder
                          .scalingPolicy(ScalingPolicy.fixed(8))
                          .build,
                        scope
                      )
      _ <- ZIO.when(streamCreated)(printLine(s"Stream $streamName just created"))
    } yield ()
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
    _ <- PravegaAdmin.readerGroup("zio-scope", group, readerSettings, "zio-stream")

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
