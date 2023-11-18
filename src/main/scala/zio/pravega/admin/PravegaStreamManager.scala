package zio.pravega.admin

import zio._
import io.pravega.client.ClientConfig
import io.pravega.client.admin.StreamManager
import io.pravega.client.stream.StreamConfiguration
import io.pravega.client.stream.StreamCut

/**
 * PravegaStreamManager is a wrapper around the StreamManager Java API.
 *
 * Basically it's allow to create, drop, seal, truncate, etc. streams.
 */
@Accessible
trait PravegaStreamManager {
  /*
   * Create a scope with the given name. Note: This method is idempotent
   * assuming called with the same name. This method may block.
   *
   * Will return false if the scope already exists, hence true if it was effectively created.
   */
  def createScope(scope: String): Task[Boolean]

  /**
   * Drop a scope with the given name. This method may block.
   *
   * Will return false if the scope does not exist, hence true if it was
   * effectively dropped.
   */
  def dropScope(scope: String): Task[Boolean]

  /**
   * Create a stream with the given name and config. Note: This method is
   * idempotent, may block.
   *
   * Will return false if the stream already exists, hence true if it was
   * effectively created.
   */
  def createStream(scope: String, streamName: String, config: StreamConfiguration): Task[Boolean]

  /**
   * Seal a stream with the given name. This method may block.
   *
   * Will return false if the stream does not exist, hence true if it was
   */
  def sealStream(scope: String, streamName: String): Task[Boolean]
  def dropStream(scope: String, streamName: String): Task[Boolean]
  def truncateStream(scope: String, streamName: String, streamCut: StreamCut): Task[Boolean]

}

object PravegaStreamManager {
  def live: ZLayer[Scope & ClientConfig, Throwable, PravegaStreamManager] = ZLayer.fromZIO(
    ZIO.serviceWithZIO[ClientConfig](clientConfig =>
      ZIO
        .attemptBlocking(StreamManager.create(clientConfig))
        .withFinalizerAuto
        .map(streamManager => PravegaStreamManagerLive(streamManager))
    )
  )
  def live(clientConfig: ClientConfig): ZLayer[Scope, Throwable, PravegaStreamManager] = ZLayer.fromZIO(
    ZIO
      .attemptBlocking(StreamManager.create(clientConfig))
      .withFinalizerAuto
      .map(streamManager => PravegaStreamManagerLive(streamManager))
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
  def truncateStream(scope: String, streamName: String, streamCut: StreamCut): RIO[PravegaStreamManager, Boolean] =
    ZIO.serviceWithZIO[PravegaStreamManager](_.truncateStream(scope, streamName, streamCut))
}

final private case class PravegaStreamManagerLive(streamManager: StreamManager) extends PravegaStreamManager {

  def createScope(scope: String): Task[Boolean] = ZIO.attemptBlocking(streamManager.createScope(scope))
  def createStream(scope: String, streamName: String, config: StreamConfiguration): Task[Boolean] = for (
    exists <- ZIO.attemptBlocking(streamManager.checkStreamExists(scope, streamName));
    created <- exists match {
                 case true =>
                   ZIO.succeed(false)
                 case false =>
                   ZIO.attemptBlocking(streamManager.createStream(scope, streamName, config))
               }
  ) yield created
  def sealStream(scope: String, streamName: String): Task[Boolean] =
    ZIO.attemptBlocking(streamManager.sealStream(scope, streamName))
  def dropStream(scope: String, streamName: String): Task[Boolean] =
    ZIO.attemptBlocking(streamManager.deleteStream(scope, streamName))
  def dropScope(scope: String): Task[Boolean] = ZIO.attemptBlocking(streamManager.deleteScope(scope))

  def truncateStream(scope: String, streamName: String, streamCut: StreamCut): Task[Boolean] =
    ZIO.attemptBlocking(streamManager.truncateStream(scope, streamName, streamCut))

}
