package zio.pravega

import zio._
import io.pravega.client.ClientConfig
import io.pravega.client.admin.StreamManager

object PravegaStreamManager {
  def live: ZLayer[Scope & ClientConfig, Throwable, StreamManager] = ZLayer.fromZIO(
    ZIO.serviceWithZIO[ClientConfig](clientConfig =>
      ZIO.attemptBlocking(StreamManager.create(clientConfig)).withFinalizerAuto
    )
  )
}
