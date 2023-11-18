package zio.pravega.admin

import io.pravega.client.admin.ReaderGroupManager
import zio.ZLayer
import io.pravega.client.stream.Stream
import io.pravega.client.ClientConfig
import scala.jdk.CollectionConverters._

import zio._

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

  def openReaderGroup(readerGroupName: String): RIO[Scope, ReaderGroup]

  def dropReaderGroup(scope: String, readerGroupName: String): Task[Boolean]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def readerOffline(groupName: String): Task[Int]

}

final case class PravegaReaderGroupManagerLive(scope: String, readerGroupManager: ReaderGroupManager)
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
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
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
  def createReaderGroup[A](
    readerGroupName: String,
    streamNames: String*
  ): ZIO[PravegaReaderGroupManager, Throwable, Boolean] = ZIO.serviceWithZIO[PravegaReaderGroupManager](
    _.createReaderGroup(readerGroupName, ReaderGroupConfig.builder(), streamNames: _*)
  )
  def readerOffline(groupName: String): RIO[PravegaReaderGroupManager, Int] =
    ZIO.serviceWithZIO[PravegaReaderGroupManager](_.readerOffline(groupName))
  def createReaderGroup(
    readerGroupName: String,
    builder: ReaderGroupConfig.ReaderGroupConfigBuilder,
    streamNames: String*
  ): RIO[PravegaReaderGroupManager, Boolean] =
    ZIO.serviceWithZIO[PravegaReaderGroupManager](_.createReaderGroup(readerGroupName, builder, streamNames: _*))
  def openReaderGroup(readerGroupName: String): RIO[PravegaReaderGroupManager with Scope, ReaderGroup] =
    ZIO.serviceWithZIO[PravegaReaderGroupManager](_.openReaderGroup(readerGroupName))
  def dropReaderGroup(scope: String, readerGroupName: String): RIO[PravegaReaderGroupManager, Boolean] =
    ZIO.serviceWithZIO[PravegaReaderGroupManager](_.dropReaderGroup(scope, readerGroupName))
}
