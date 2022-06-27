---
sidebar_position: 2
---
# Stream

```scala mdoc:invisible
import zio.Console._
import zio.stream._
import zio.pravega._
import io.pravega.client.stream.impl.UTF8StringSerializer

val writerSettings =
    WriterSettingsBuilder()
      .withSerializer(new UTF8StringSerializer)

val readerSettings =
    ReaderSettingsBuilder()
      .withSerializer(new UTF8StringSerializer)


```

## Stream writer

* Without transaction in a stream, simply create a Sink:

```scala mdoc:silent
val sink = PravegaStream.sink("my-stream", writerSettings)
```
* With transaction, the transaction will commit at the end of the Stream, or rollback if an error is raised.

```scala mdoc:silent
val sinkTx = PravegaStream.sinkTx("my-stream", writerSettings)
```

## Stream reader

To read from a stream, simply create a stream:

```scala mdoc:silent
val stream = PravegaStream.stream("mygroup", readerSettings)
```


# All together 


```scala mdoc:silent
// A Stream of strings
def testStream(a: Int, b: Int): ZStream[Any, Nothing, String] =
    ZStream.fromIterable(a until b).map(i => s"ZIO Message $i")

val n = 10

for {
      sink <- PravegaStream.sink("my-stream", writerSettings)
      _ <- testStream(0, 10).run(sink)
      stream <- PravegaStream.stream("my-group", readerSettings)
      count <- stream
        .take(n.toLong * 2)
        .tap(e => printLine(s"ZStream of [$e]"))
        .runFold(0)((s, _) => s + 1)
    } yield count
```