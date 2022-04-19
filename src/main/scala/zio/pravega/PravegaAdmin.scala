package zio.pravega

import zio._

import io.pravega.client.ClientConfig
import io.pravega.client.admin.StreamManager
import io.pravega.client.admin.ReaderGroupManager

import scala.jdk.CollectionConverters._
import io.pravega.client.stream.StreamConfiguration

import io.pravega.client.stream.ReaderGroupConfig
import io.pravega.client.stream.Stream
import io.pravega.client.tables.KeyValueTableConfiguration
import io.pravega.client.admin.KeyValueTableManager

trait PravegaAdminService {
  def readerGroup[A](
      scope: String,
      readerGroupName: String,
      streamNames: String*
  ): ZIO[Any, Throwable, Boolean]

  def createScope(scope: String): RIO[Scope, Boolean]

  def createStream(
      streamName: String,
      config: StreamConfiguration,
      scope: String
  ): RIO[Scope, Boolean]

  def readerGroupManager(
      scope: String
  ): RIO[Scope, ReaderGroupManager]

  def readerGroupManager(
      scope: String,
      clientConfig: ClientConfig
  ): RIO[Scope, ReaderGroupManager]

  def streamManager(): RIO[Scope, StreamManager]

  def keyValueTableManager(): RIO[Scope, KeyValueTableManager]

  def createTable(
      tableName: String,
      config: KeyValueTableConfiguration,
      scope: String
  ): RIO[Scope, Boolean]

  def readerOffline(
      scope: String,
      groupName: String
  ): RIO[Scope, Int]

}

object PravegaAdmin extends Accessible[PravegaAdminService]

case class PravegaAdmin(clientConfig: ClientConfig)
    extends PravegaAdminService {

  def readerGroup[A](
      scope: String,
      readerGroupName: String,
      streamNames: String*
  ): ZIO[Any, Throwable, Boolean] = ZIO.scoped {

    def config = streamNames
      .foldLeft(ReaderGroupConfig.builder()) { case (builder, streamName) =>
        builder.stream(Stream.of(scope, streamName))
      }

    for {
      manager <- readerGroupManager(scope)
      created <- ZIO.attemptBlocking {
        manager.createReaderGroup(
          readerGroupName,
          config.build()
        )
      }
    } yield created

  }

  def createScope(scope: String): RIO[Scope, Boolean] =
    for {
      streamManager <- streamManager()
      exists <- ZIO.attemptBlocking(streamManager.checkScopeExists(scope))
      created <- exists match {
        case true  => ZIO.succeed(false)
        case false => ZIO.attemptBlocking(streamManager.createScope(scope))
      }
    } yield created

  def createStream(
      streamName: String,
      config: StreamConfiguration,
      scope: String
  ): RIO[Scope, Boolean] =
    for {
      streamManager <- streamManager()

      exists <- ZIO.attemptBlocking(
        streamManager.checkStreamExists(scope, streamName)
      )
      created <- exists match {
        case true => ZIO.succeed(false)
        case false =>
          ZIO.attemptBlocking(
            streamManager.createStream(scope, streamName, config)
          )
      }
    } yield created

  def readerGroupManager(
      scope: String
  ): RIO[Scope, ReaderGroupManager] =
    ZIO
      .attemptBlocking(ReaderGroupManager.withScope(scope, clientConfig))
      .withFinalizerAuto

  def readerGroupManager(
      scope: String,
      clientConfig: ClientConfig
  ): RIO[Scope, ReaderGroupManager] =
    ZIO
      .attemptBlocking(ReaderGroupManager.withScope(scope, clientConfig))
      .withFinalizerAuto

  def streamManager(): RIO[Scope, StreamManager] =
    ZIO.attemptBlocking(StreamManager.create(clientConfig)).withFinalizerAuto

  override def keyValueTableManager(): RIO[Scope, KeyValueTableManager] =
    ZIO
      .attemptBlocking(KeyValueTableManager.create(clientConfig))
      .withFinalizerAuto

  override def createTable(
      tableName: String,
      config: KeyValueTableConfiguration,
      scope: String
  ): RIO[Scope, Boolean] =
    for {
      keyValueTableManager <- keyValueTableManager()
      created <- ZIO.attemptBlocking(
        keyValueTableManager.createKeyValueTable(scope, tableName, config)
      )
    } yield created

  def readerOffline(
      scope: String,
      groupName: String
  ): RIO[Scope, Int] =
    for {
      groupManager <- readerGroupManager(scope, clientConfig)
      group <- ZIO
        .attemptBlocking(groupManager.getReaderGroup(groupName))
        .withFinalizerAuto
      freed <-
        ZIO.foreach(group.getOnlineReaders().asScala.toSeq)(id =>
          ZIO.attemptBlocking(group.readerOffline(id, null))
        )

    } yield freed.size

}

object PravegaAdminLayer {
  def layer: ZLayer[ClientConfig, Nothing, PravegaAdmin] =
    ZLayer(for {
      clientConfig <- ZIO.service[ClientConfig]
      l <- ZIO.succeed(new PravegaAdmin(clientConfig))

    } yield l)
}
