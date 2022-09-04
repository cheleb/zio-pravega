package zio.pravega.docs

import zio._
import zio.Console._
import zio.pravega._

import io.pravega.client.stream.StreamConfiguration
import io.pravega.client.stream.ScalingPolicy

object CreateResourcesExample extends ZIOAppDefault {

  private val streamConfiguration = StreamConfiguration.builder
    .scalingPolicy(ScalingPolicy.fixed(8))
    .build

  private val program = for {
    scopeCreated <- PravegaAdmin.createScope("a-scope")
    _ <- printLine(s"Scope created: $scopeCreated")
    streamCreated <- PravegaAdmin.createStream(
      "a-scope",
      "a-stream",
      streamConfiguration
    )
    _ <- printLine(s"Stream created: $streamCreated")
  } yield ()

  override def run: ZIO[Scope, Throwable, Unit] =
    program
      .provideSome[Scope](
        PravegaAdmin.live(PravegaClientConfig.default)
      )

}
