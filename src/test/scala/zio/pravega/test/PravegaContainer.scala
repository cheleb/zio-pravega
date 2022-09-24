package zio.pravega.test

import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName

import java.time.Duration
import org.testcontainers.containers.wait.strategy.Wait
import zio._

import zio.pravega.PravegaClientConfig
import io.pravega.client.ClientConfig

class PravegaContainer(
    dockerImageName: DockerImageName
) extends GenericContainer[PravegaContainer](dockerImageName) {
  withStartupTimeout(Duration.ofMinutes(2))
  addFixedExposedPort(9090, 9090)
  addFixedExposedPort(12345, 12345)
  waitingFor(
    Wait.forLogMessage(
      ".*Pravega Sandbox is running locally now. You could access it at 127.0.0.1:9090.*",
      1
    )
  )
  withCommand("standalone")

}

object PravegaContainer {
  val pravega: ZLayer[Any, Nothing, PravegaContainer] = {
    val imageName = sys.env.getOrElse("PRAVEGA_IMAGE", "pravega/pravega:0.12.0")
    ZLayer.scoped(ZIO.acquireRelease {
      ZIO.attemptBlocking {
        val container = new PravegaContainer(
          dockerImageName = DockerImageName.parse(imageName)
        )
        container.start()
        container
      }.orDie
    }(container => ZIO.attemptBlocking(container.stop()).orDie))
  }
  def clientConfig: ZLayer[PravegaContainer, Nothing, ClientConfig] =
    ZLayer.succeed(PravegaClientConfig.default)
}
// object PravegaContainer {
//   val pravega: ZLayer[Any, Nothing, PravegaContainer] = {
//     val imageName = sys.env.getOrElse("PRAVEGA_IMAGE", "pravega/pravega:0.12.0")

//     ZLayer.scoped(ZIO.acquireRelease {
//       Ref.make(0).map { _ =>
//         val container = new PravegaContainer(
//           dockerImageName = DockerImageName.parse(imageName)
//         )
//         container.start()
//         container
//       }
//     }(ref => ZIO.attemptBlocking(ref.stop()).orDie))
//   }
//   def clientConfig: ZLayer[PravegaContainer, Nothing, ClientConfig] =
//     ZLayer.succeed(PravegaClientConfig.default)
// }
