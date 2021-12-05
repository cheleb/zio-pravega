package zio.pravega.test

import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName

import java.time.Duration
import org.testcontainers.containers.wait.strategy.Wait

class PravegaContainer(
    dockerImageName: DockerImageName =
      DockerImageName.parse("pravega/pravega:0.10.1")
) extends GenericContainer[PravegaContainer](dockerImageName) {

//  withNetworkMode("host")
//  withEnv("HOST_IP", "192.168.1.69")
//  withEnv("HOST_IP", "docker.for.mac.localhost")
  withEnv("HOST_IP", "localdocker")
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
