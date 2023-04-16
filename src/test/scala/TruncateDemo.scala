import zio._
import zio.Console._

import io.pravega.client.ClientConfig

import zio.pravega.admin.PravegaReaderGroupManager
import zio.pravega.admin.PravegaStreamManager
import io.pravega.client.stream.StreamConfiguration
import io.pravega.client.stream.ScalingPolicy
import zio.pravega.PravegaStream
import zio.pravega.WriterSettingsBuilder
import io.pravega.client.stream.impl.UTF8StringSerializer
import zio.stream.ZStream
import zio.pravega.ReaderSettingsBuilder
import scala.jdk.CollectionConverters._

object TruncateDemo extends ZIOAppDefault {

  private val clientConfig = ClientConfig.builder().build()

  private val program = for {
    // _           <- PravegaStreamManager.createScope("a-scope")
    // streamConfig = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(3)).build()
    // _           <- PravegaStreamManager.createStream("a-scope", "a-stream", streamConfig)

    _ <- PravegaReaderGroupManager.createReaderGroup("a-reader-group2", "a-stream")

    readerGroup <- PravegaReaderGroupManager.openReaderGroup("a-reader-group2")

    // writterSettings = WriterSettingsBuilder()
    //                     .withSerializer(new UTF8StringSerializer)

    readerSettings = ReaderSettingsBuilder().withSerializer(new UTF8StringSerializer)

    // sink = PravegaStream.sink("a-stream", writterSettings)

    // _ <- ZStream
    //        .fromIterable(1 to 100)
    //        .map(_.toString)
    //        .run(sink)

    _ <- PravegaStream.stream(readerGroup.getGroupName(), readerSettings).take(10).tap(ZIO.debug(_)).runDrain

    streamCuts = readerGroup.getStreamCuts()

    // _ <- ZIO.foreach(streamCuts.asScala.toList) { case (stream, streamCut) =>
    //        ZIO.debug(s"Stream: $stream, StreamCut: $streamCut") *>
    //          PravegaStreamManager.truncateStream("a-scope", stream.getStreamName(), streamCut)
    //      }

    _ <- ZIO.debug(streamCuts)

  } yield ()

  override def run = program
    .provide(
      Scope.default,
      ZLayer.succeed(clientConfig),
      PravegaReaderGroupManager.live("a-scope"),
//      PravegaReaderGroupManager.live("a-scope"),
      PravegaStreamManager.live,
      PravegaStream.fromScope("a-scope")
    )

}
