package zio.pravega.admin

import io.pravega.client.admin.ReaderGroupManager
import io.pravega.client.stream.Stream
import io.pravega.client.ClientConfig
import scala.jdk.CollectionConverters._

import zio.*

import io.pravega.client.stream.ReaderGroupConfig
import io.pravega.client.stream.ReaderGroup

/**
 * PravegaReaderGroupManager is a wrapper around the ReaderGroupManager Java
 * API. It provides a ZIO interface to the ReaderGroupManager API.
 */
@Accessible
trait PravegaReaderGroupManager {

  /**
   * Create a reader group with the given name and stream names. Note: This
   * method is idempotent assuming called with the same name and config. This
   * method may block.
   *
   * Will fail if the reader group already exists and the config is different.
   */
  def createReaderGroup(
    readerGroupName: String,
    builder: ReaderGroupConfig.ReaderGroupConfigBuilder,
    streamNames: String*
  ): Task[Boolean]

  /**
   * Open a reader group with the given name. This method may block.
   */
  def openReaderGroup(readerGroupName: String): RIO[Scope, ReaderGroup]

  /**
   * Drop a reader group with the given name. This method may block.
   */
  def dropReaderGroup(scope: String, readerGroupName: String): Task[Boolean]

  /**
   * Mark a reader as offline. This method may block.
   *
   * This can be used to force a reader to be re-created. This is useful if a
   * reader is stuck in a bad state.
   */
  def readerOffline(groupName: String): Task[Int]

}

/**
 * PravegaReaderGroupManagerLive is the live implementation of the
 * PravegaReaderGroupManager interface.
 */
private final case class PravegaReaderGroupManagerLive(scope: String, readerGroupManager: ReaderGroupManager)
    extends PravegaReaderGroupManager {
  def openReaderGroup(readerGroupName: String): RIO[Scope, ReaderGroup] =
    ZIO.attemptBlocking(readerGroupManager.getReaderGroup(readerGroupName)).withFinalizerAuto
  def dropReaderGroup(scope: String, readerGroupName: String): Task[Boolean] =
    ZIO.attemptBlocking(readerGroupManager.deleteReaderGroup(readerGroupName)).as(true)
  def createReaderGroup(
    readerGroupName: String,
    builder: ReaderGroupConfig.ReaderGroupConfigBuilder,
    streamNames: String*
  ): Task[Boolean] = {
    val config = streamNames.foldLeft(builder) { case (builder, streamName) =>
      builder.stream(Stream.of(scope, streamName))
    }
    ZIO.attemptBlocking(readerGroupManager.createReaderGroup(readerGroupName, config.build()))
  }

  def readerOffline(groupName: String): Task[Int] = ZIO.scoped {
    for (
      group <- ZIO.attemptBlocking(readerGroupManager.getReaderGroup(groupName)).withFinalizerAuto;
      freed <-
        ZIO.foreach(group.getOnlineReaders().asScala.toSeq)(id => ZIO.attemptBlocking(group.readerOffline(id, null)))
    ) yield freed.size
  }
}

object PravegaReaderGroupManager {
  def live(scope: String): RLayer[Scope & ClientConfig, PravegaReaderGroupManager] = ZLayer.fromZIO(
    ZIO.serviceWithZIO[ClientConfig](clientConfig =>
      ZIO
        .attemptBlocking(ReaderGroupManager.withScope(scope, clientConfig))
        .withFinalizerAuto
        .map(rgm => PravegaReaderGroupManagerLive(scope, rgm))
    )
  )
  def live(scope: String, clientConfig: ClientConfig): RLayer[Scope, PravegaReaderGroupManager] = ZLayer.fromZIO(
    ZIO
      .attemptBlocking(ReaderGroupManager.withScope(scope, clientConfig))
      .withFinalizerAuto
      .map(rgm => PravegaReaderGroupManagerLive(scope, rgm))
  )

  /**
   * Create a reader group with the given name and stream names.
   *
   * Note: This method is idempotent assuming called with the same name and
   * config, but it will fail if the reader group already exists and the config
   * is different.
   *
   * This method may block.
   *
   * @param readerGroupName
   * @param streamNames
   * @return
   */
  def createReaderGroup[A](
    readerGroupName: String,
    streamNames: String*
  ): ZIO[PravegaReaderGroupManager, Throwable, Boolean] = ZIO.serviceWithZIO[PravegaReaderGroupManager](
    _.createReaderGroup(readerGroupName, ReaderGroupConfig.builder(), streamNames*)
  )

  /**
   * Create a reader group with the given name and stream names.
   *
   * Note: This method is idempotent assuming called with the same name and
   * config, but it will fail if the reader group already exists and the config
   * is different.
   *
   * This method may block.
   *
   * @param readerGroupName
   * @param builder
   *   A ReaderGroupConfigBuilder to configure the reader group
   * @param streamNames
   * @return
   */
  def createReaderGroup(
    readerGroupName: String,
    builder: ReaderGroupConfig.ReaderGroupConfigBuilder,
    streamNames: String*
  ): RIO[PravegaReaderGroupManager, Boolean] =
    ZIO.serviceWithZIO[PravegaReaderGroupManager](_.createReaderGroup(readerGroupName, builder, streamNames*))

  /**
   * Open a reader group with the given name.
   */
  def openReaderGroup(readerGroupName: String): RIO[PravegaReaderGroupManager & Scope, ReaderGroup] =
    ZIO.serviceWithZIO[PravegaReaderGroupManager](_.openReaderGroup(readerGroupName))

  /**
   * Drop a reader group with the given name.
   */
  def dropReaderGroup(scope: String, readerGroupName: String): RIO[PravegaReaderGroupManager, Boolean] =
    ZIO.serviceWithZIO[PravegaReaderGroupManager](_.dropReaderGroup(scope, readerGroupName))

  /**
   * Remove a reader group with the given name.
   *
   * If some reason a Reader exited without releasing resource, calling this
   * method can help.
   *
   * This method may block.
   *
   * @param groupName
   * @return
   */
  def readerOffline(groupName: String): RIO[PravegaReaderGroupManager, Int] =
    ZIO.serviceWithZIO[PravegaReaderGroupManager](_.readerOffline(groupName))

}
