package zio.pravega.test

import zio._
import org.testcontainers.utility.DockerImageName

object TestContainer {
  type Pravega = PravegaContainer

  def pravega(
      imageName: String
  ): ZLayer[Any, Nothing, Pravega] =
    ZManaged.acquireReleaseWith {
      ZIO.attemptBlocking {
        val container = new PravegaContainer(
          dockerImageName = DockerImageName.parse(imageName)
        )
        container.start()
        container
      }.orDie
    }(container => ZIO.attemptBlocking(container.stop()).orDie).toLayer
}
