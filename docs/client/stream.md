# Stream

```scala mdoc
import zio.Console._
import zio.stream._
import zio.pravega._
import io.pravega.client.stream.impl.UTF8StringSerializer


```

## Producer

To write in a stream, simply bread a Sink:

```scala mdoc
val writterSettings =
    WriterSettingsBuilder()
      .withSerializer(new UTF8StringSerializer)
val sink = PravegaService(_.sink("my-stream", writterSettings))
```
## Consumer

### Group name

```scala mdoc
val group = PravegaAdminService(
        _.readerGroup(
          "my-scope",
          "my-group",
          "my-stream"
        )
      )
```

### Stream reader

```scala mdoc
val readerSettings =
    ReaderSettingsBuilder()
      .withSerializer(new UTF8StringSerializer)
val stream = PravegaService(_.stream("mygroup", readerSettings))
```


# All together 


```scala mdoc 

def testStream(a: Int, b: Int): ZStream[Any, Nothing, String] =
    Stream.fromIterable(a until b).map(i => s"ZIO Message $i")

val n = 10

for {
      sink <- PravegaService(_.sink("my-stream", writterSettings))
      _ <- testStream(0, 10).run(sink)
      _ <- group
      stream <- PravegaService(_.stream("my-group", readerSettings))
      count <- stream
        .take(n.toLong * 2)
        .tap(e => printLine(s"ZStream of [$e]"))
        .runFold(0)((s, _) => s + 1)
    } yield count
```