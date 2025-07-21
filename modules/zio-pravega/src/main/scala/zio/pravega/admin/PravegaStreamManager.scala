package zio.pravega.admin

import zio._
import io.pravega.client.ClientConfig
import io.pravega.client.admin.StreamManager
import io.pravega.client.stream.{Stream => PravegaJavaStream}
import io.pravega.client.stream.StreamConfiguration
import io.pravega.client.stream.StreamCut
import zio.stream.ZStream

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
   * List all the scopes in the Pravega cluster. This method may block.
   */
  def listScopes: ZStream[Scope, Throwable, String]

  /**
   * Drop a scope with the given name. This method may block.
   *
   * Will return false if the scope does not exist, hence true if it was
   * effectively dropped.
   */
  def deleteScope(scope: String): Task[Boolean]

  /**
   * Create a stream with the given name and config. Note: This method is
   * idempotent, may block.
   *
   * Will return false if the stream already exists, hence true if it was
   * effectively created.
   */
  def createStream(scope: String, streamName: String, config: StreamConfiguration): Task[Boolean]

  def listStreams(scope: String): ZStream[Any, Throwable, PravegaJavaStream]

  /**
   * Seal a stream with the given name. This method may block.
   *
   * Will return false if the stream does not exist, hence true if it was
   */
  def sealStream(scope: String, streamName: String): Task[Boolean]

  /**
   * Drop a stream with the given name. This method may block.
   */
  def deleteStream(scope: String, streamName: String): Task[Boolean]

  /**
   * Truncate a stream with the given name. This method may block.
   */
  def truncateStream(scope: String, streamName: String, streamCut: StreamCut): Task[Boolean]

}

/**
 * PravegaStreamManager is a wrapper around the StreamManager Java API.
 *
 * Basically it's allow to create, drop, seal, truncate, etc. streams.
 */
object PravegaStreamManager {

  /**
   * ZIO to create a PravegaStreamManager from a ClientConfig.
   */
  private def streamManager(clientConfig: ClientConfig): ZIO[Scope, Throwable, PravegaStreamManagerLive] =
    ZIO
      .attemptBlocking(StreamManager.create(clientConfig))
      .withFinalizerAuto
      .map(streamManager => PravegaStreamManagerLive(streamManager))

  /**
   * ZLayer to provide a PravegaStreamManager from a environment with a
   * ClientConfig.
   */
  def live: ZLayer[Scope & ClientConfig, Throwable, PravegaStreamManager] = ZLayer.fromZIO(
    ZIO.serviceWithZIO[ClientConfig](streamManager)
  )

  /**
   * ZLayer to provide a PravegaStreamManager from a ClientConfig.
   */
  def live(clientConfig: ClientConfig): ZLayer[Scope, Throwable, PravegaStreamManager] = ZLayer.fromZIO(
    streamManager(clientConfig)
  )

  /**
   * Create a scope with the given name.
   *
   * Returns true if the scope was created, false if it already exists.
   */
  def createScope(scope: String): RIO[PravegaStreamManager, Boolean] =
    ZIO.serviceWithZIO[PravegaStreamManager](_.createScope(scope))

  /**
   * List all the scopes in the Pravega cluster.
   */
  def listScopes: ZStream[Scope & PravegaStreamManager, Throwable, String] =
    ZStream.serviceWithStream[PravegaStreamManager](_.listScopes)

  /**
   * Drop a scope with the given name.
   *
   * Returns true if the scope was dropped, false if it does not exist.
   */
  def deleteScope(scope: String): RIO[PravegaStreamManager, Boolean] =
    ZIO.serviceWithZIO[PravegaStreamManager](_.deleteScope(scope))

  /**
   * Create a stream with the given name and config.
   *
   * Returns true if the stream was created, false if it already exists.
   */
  def createStream(scope: String, streamName: String, config: StreamConfiguration): RIO[PravegaStreamManager, Boolean] =
    ZIO.serviceWithZIO[PravegaStreamManager](_.createStream(scope, streamName, config))

  /**
   * List all the streams in the Pravega cluster given scope.
   */
  def listStreams(scope: String): ZStream[PravegaStreamManager, Throwable, PravegaJavaStream] =
    ZStream.serviceWithStream[PravegaStreamManager](_.listStreams(scope))

  /**
   * Seal a stream with the given name.
   *
   * Returns true if the stream is sealed.
   */
  def sealStream(scope: String, streamName: String): RIO[PravegaStreamManager, Boolean] =
    ZIO.serviceWithZIO[PravegaStreamManager](_.sealStream(scope, streamName))

  /**
   * Drop a stream with the given name.
   *
   * Returns true if the stream is deleted.
   */
  def deleteStream(scope: String, streamName: String): RIO[PravegaStreamManager, Boolean] =
    ZIO.serviceWithZIO[PravegaStreamManager](_.deleteStream(scope, streamName))

  /**
   * Truncate a stream with the given name.
   *
   * Returns true if the stream is truncated.
   */
  def truncateStream(scope: String, streamName: String, streamCut: StreamCut): RIO[PravegaStreamManager, Boolean] =
    ZIO.serviceWithZIO[PravegaStreamManager](_.truncateStream(scope, streamName, streamCut))
}

/*
 * PravegaStreamManagerLive is the implementation of PravegaStreamManager.
 *
 * It's a wrapper around the StreamManager Java API.
 *
 * Basically it's allow to create, drop, seal, truncate, etc. streams and scope.
 */
final private case class PravegaStreamManagerLive(streamManager: StreamManager) extends PravegaStreamManager {

  /**
   * Create a scope with the given name.
   *
   * Returns true if the scope was created, false if it already exists.
   */
  def createScope(scope: String): Task[Boolean] = ZIO.attemptBlocking(streamManager.createScope(scope))

  /**
   * List all the scopes in the Pravega cluster.
   */
  def listScopes: ZStream[Scope, Throwable, String] = ZStream.fromJavaIterator(streamManager.listScopes())

  /**
   * Create a stream with the given name and config.
   *
   * Returns true if the stream was created, false if it already exists.
   */
  def createStream(scope: String, streamName: String, config: StreamConfiguration): Task[Boolean] = for (
    exists  <- ZIO.attemptBlocking(streamManager.checkStreamExists(scope, streamName));
    created <- exists match {
                 case true =>
                   ZIO.succeed(false)
                 case false =>
                   ZIO.attemptBlocking(streamManager.createStream(scope, streamName, config))
               }
  ) yield created

  /**
   * List all the streams in the Pravega cluster given scope.
   */

  def listStreams(scope: String): ZStream[Any, Throwable, PravegaJavaStream] =
    ZStream.fromJavaIterator(streamManager.listStreams(scope))

  /**
   * Seal a stream with the given name.
   *
   * Returns true if the stream is sealed.
   */
  def sealStream(scope: String, streamName: String): Task[Boolean] =
    ZIO.attemptBlocking(streamManager.sealStream(scope, streamName))

  /**
   * Drop a stream with the given name.
   *
   * Returns true if the stream is deleted.
   */
  def deleteStream(scope: String, streamName: String): Task[Boolean] =
    ZIO.attemptBlocking(streamManager.deleteStream(scope, streamName))

  /**
   * Drop a scope with the given name.
   *
   * Returns true if the scope is deleted.
   */
  def deleteScope(scope: String): Task[Boolean] = ZIO.attemptBlocking(streamManager.deleteScope(scope))

  /**
   * Truncate a stream with the given name.
   *
   * Returns true if the stream is truncated.
   */
  def truncateStream(scope: String, streamName: String, streamCut: StreamCut): Task[Boolean] =
    ZIO.attemptBlocking(streamManager.truncateStream(scope, streamName, streamCut))

}
