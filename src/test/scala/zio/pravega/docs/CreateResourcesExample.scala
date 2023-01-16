package zio.pravega.docs

import zio._
import zio.Console._
import zio.pravega.PravegaClientConfig
import zio.pravega.admin._

import io.pravega.client.stream.StreamConfiguration
import io.pravega.client.stream.ScalingPolicy

object CreateResourcesExample extends ZIOAppDefault {

  private val streamConfiguration = StreamConfiguration.builder.scalingPolicy(ScalingPolicy.fixed(1)).build

  private val program = for {
    scopeCreated  <- PravegaStreamManager.createScope("a-scope")
    _             <- printLine(f"Scope created: $scopeCreated%b")
    streamCreated <- PravegaStreamManager.createStream("a-scope", "a-stream", streamConfiguration)
    _             <- printLine(f"Stream created: $streamCreated%b")
    _             <- PravegaReaderGroupManager.createReaderGroup("a-reader-group", "a-stream")
  } yield ()

  override def run: ZIO[Scope, Throwable, Unit] = program
    .provideSome[Scope](
      PravegaClientConfig.live,
      PravegaReaderGroupManager.live("a-scope"),
      PravegaStreamManager.live
    )

}
