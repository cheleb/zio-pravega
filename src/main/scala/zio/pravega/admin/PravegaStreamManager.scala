package zio.pravega.admin

import zio._
import io.pravega.client.ClientConfig
import io.pravega.client.admin.StreamManager
import io.pravega.client.stream.StreamConfiguration

@Accessible
trait PravegaStreamManager {
  def createScope(scope: String): Task[Boolean]
  def dropScope(scope: String): Task[Boolean]
  def createStream(scope: String, streamName: String, config: StreamConfiguration): Task[Boolean]
  def sealStream(scope: String, streamName: String): Task[Boolean]

  def dropStream(scope: String, streamName: String): Task[Boolean]

}

object PravegaStreamManager {
  def live(clientConfig: ClientConfig): ZLayer[Scope, Throwable, PravegaStreamManager] = ZLayer.fromZIO(
    ZIO.attemptBlocking(StreamManager.create(clientConfig)).withFinalizerAuto.map(PravegaStreamManagerLive(_))
  )
  def live: ZLayer[Scope & ClientConfig, Throwable, PravegaStreamManager] = ZLayer.fromZIO(
    ZIO.serviceWithZIO[ClientConfig](clientConfig =>
      ZIO.attemptBlocking(StreamManager.create(clientConfig)).withFinalizerAuto.map(PravegaStreamManagerLive(_))
    )
  )
  def createScope(scope: String): RIO[PravegaStreamManager, Boolean] =
    ZIO.serviceWithZIO[PravegaStreamManager](_.createScope(scope))
  def dropScope(scope: String): RIO[PravegaStreamManager, Boolean] =
    ZIO.serviceWithZIO[PravegaStreamManager](_.dropScope(scope))
  def createStream(scope: String, streamName: String, config: StreamConfiguration): RIO[PravegaStreamManager, Boolean] =
    ZIO.serviceWithZIO[PravegaStreamManager](_.createStream(scope, streamName, config))
  def sealStream(scope: String, streamName: String): RIO[PravegaStreamManager, Boolean] =
    ZIO.serviceWithZIO[PravegaStreamManager](_.sealStream(scope, streamName))
  def dropStream(scope: String, streamName: String): RIO[PravegaStreamManager, Boolean] =
    ZIO.serviceWithZIO[PravegaStreamManager](_.dropStream(scope, streamName))
}

private case class PravegaStreamManagerLive(streamManager: StreamManager) extends PravegaStreamManager {

  override def sealStream(scope: String, streamName: String): Task[Boolean] =
    ZIO.attemptBlocking(streamManager.sealStream(scope, streamName))

  override def dropStream(scope: String, streamName: String): Task[Boolean] =
    ZIO.attemptBlocking(streamManager.deleteStream(scope, streamName))

  def dropScope(scope: String): Task[Boolean] = ZIO.attemptBlocking(streamManager.deleteScope(scope))

  override def createStream(scope: String, streamName: String, config: StreamConfiguration): Task[Boolean] = for {
    exists <- ZIO.attemptBlocking(streamManager.checkStreamExists(scope, streamName))
    created <- exists match {
                 case true  => ZIO.succeed(false)
                 case false => ZIO.attemptBlocking(streamManager.createStream(scope, streamName, config))
               }
  } yield created

  override def createScope(scope: String): Task[Boolean] =
    ZIO.attemptBlocking(streamManager.createScope(scope))

}
