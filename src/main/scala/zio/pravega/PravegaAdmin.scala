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
import io.pravega.client.stream.ReaderGroup

trait PravegaAdminService {
  def createReaderGroup[A](
      scope: String,
      readerGroupName: String,
      builder: ReaderGroupConfig.ReaderGroupConfigBuilder,
      streamNames: String*
  ): ZIO[Scope, Throwable, Boolean]

  def openReaderGroup[A](
      scope: String,
      readerGroupName: String
  ): ZIO[Scope, Throwable, ReaderGroup]

  def createScope(scope: String): RIO[Scope, Boolean]

  def createStream(
      scope: String,
      streamName: String,
      config: StreamConfiguration
  ): RIO[Scope, Boolean]

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

object PravegaAdmin {
  def createReaderGroup[A](
      scope: String,
      readerGroupName: String,
      streamNames: String*
  ): ZIO[PravegaAdminService & Scope, Throwable, Boolean] =
    ZIO.serviceWithZIO[PravegaAdminService](
      _.createReaderGroup(
        scope,
        readerGroupName,
        ReaderGroupConfig.builder(),
        streamNames: _*
      )
    )
  def createReaderGroup[A](
      scope: String,
      readerGroupName: String,
      builder: ReaderGroupConfig.ReaderGroupConfigBuilder,
      streamNames: String*
  ): ZIO[PravegaAdminService & Scope, Throwable, Boolean] =
    ZIO.serviceWithZIO[PravegaAdminService](
      _.createReaderGroup(scope, readerGroupName, builder, streamNames: _*)
    )

  def openReaderGroup[A](
      scope: String,
      readerGroupName: String
  ): ZIO[PravegaAdminService & Scope, Throwable, ReaderGroup] =
    ZIO.serviceWithZIO[PravegaAdminService](
      _.openReaderGroup(scope, readerGroupName)
    )

  def createScope(scope: String): RIO[PravegaAdminService & Scope, Boolean] =
    ZIO.serviceWithZIO[PravegaAdminService](_.createScope(scope))

  def createStream(
      scope: String,
      streamName: String,
      config: StreamConfiguration
  ): RIO[PravegaAdminService & Scope, Boolean] =
    ZIO.serviceWithZIO[PravegaAdminService](
      _.createStream(scope, streamName, config)
    )

  def createTable(
      tableName: String,
      config: KeyValueTableConfiguration,
      scope: String
  ): RIO[PravegaAdminService & Scope, Boolean] =
    ZIO.serviceWithZIO[PravegaAdminService](
      _.createTable(tableName, config, scope)
    )

  def readerOffline(
      scope: String,
      groupName: String
  ): RIO[PravegaAdminService & Scope, Int] =
    ZIO.serviceWithZIO[PravegaAdminService](_.readerOffline(scope, groupName))

  def live(
      clientConfig: ClientConfig
  ): ZLayer[Any, Nothing, PravegaAdminService] =
    ZLayer.succeed(new PravegaAdminServiceImpl(clientConfig))

}

private class PravegaAdminServiceImpl(clientConfig: ClientConfig)
    extends PravegaAdminService {

  def createReaderGroup[A](
      scope: String,
      readerGroupName: String,
      builder: ReaderGroupConfig.ReaderGroupConfigBuilder,
      streamNames: String*
  ): ZIO[Scope, Throwable, Boolean] = {

    val config = streamNames
      .foldLeft(builder) { case (builder, streamName) =>
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

  override def openReaderGroup[A](
      scope: String,
      readerGroupName: String
  ): ZIO[Scope, Throwable, ReaderGroup] = for {
    manager <- readerGroupManager(scope)
    readerGroup <- ZIO
      .attemptBlocking(manager.getReaderGroup(readerGroupName))
      .withFinalizerAuto
  } yield readerGroup

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
      scope: String,
      streamName: String,
      config: StreamConfiguration
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

  def keyValueTableManager(): RIO[Scope, KeyValueTableManager] =
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
