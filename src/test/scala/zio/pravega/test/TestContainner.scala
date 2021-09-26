package zio.pravega.test

import zio._
import org.testcontainers.utility.DockerImageName

object TestContainer {
  type Pravega = Has[PravegaContainer]

  def pravega(
    imageName: DockerImageName = DockerImageName.parse("pravega/pravega:0.10.1")
  ): ZLayer[Any, Nothing, Pravega] =
    ZManaged.acquireReleaseWith {
      ZIO.attemptBlocking {
        val container = new PravegaContainer(
          dockerImageName = imageName
        )
        container.start()
        container
      }.orDie
    }(container => ZIO.attemptBlocking(container.stop()).orDie).toLayer
}
