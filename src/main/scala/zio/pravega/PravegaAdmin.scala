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

trait PravegaAdmin {
  def createReaderGroup[A](
      scope: String,
      readerGroupName: String,
      builder: ReaderGroupConfig.ReaderGroupConfigBuilder,
      streamNames: String*
  ): ZIO[Scope, Throwable, Boolean]

  def dropReaderGroup(
      scope: String,
      readerGroupName: String
  ): RIO[Scope, Boolean]

  def openReaderGroup[A](
      scope: String,
      readerGroupName: String
  ): ZIO[Scope, Throwable, ReaderGroup]

  def createScope(scope: String): RIO[Scope, Boolean]

  def dropScope(scope: String): RIO[Scope, Boolean]

  def createStream(
      scope: String,
      streamName: String,
      config: StreamConfiguration
  ): RIO[Scope, Boolean]

  def sealStream(scope: String, streamName: String): RIO[Scope, Boolean]

  def dropStream(scope: String, streamName: String): RIO[Scope, Boolean]

  def createTable(
      scope: String,
      tableName: String,
      config: KeyValueTableConfiguration
  ): RIO[Scope, Boolean]

  def dropTable(scope: String, tableName: String): RIO[Scope, Boolean]

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
  ): ZIO[PravegaAdmin & Scope, Throwable, Boolean] =
    ZIO.serviceWithZIO[PravegaAdmin](
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
  ): ZIO[PravegaAdmin & Scope, Throwable, Boolean] =
    ZIO.serviceWithZIO[PravegaAdmin](
      _.createReaderGroup(scope, readerGroupName, builder, streamNames: _*)
    )

  def openReaderGroup[A](
      scope: String,
      readerGroupName: String
  ): ZIO[PravegaAdmin & Scope, Throwable, ReaderGroup] =
    ZIO.serviceWithZIO[PravegaAdmin](
      _.openReaderGroup(scope, readerGroupName)
    )

  def dropReaderGroup(
      scope: String,
      readerGroupName: String
  ): RIO[PravegaAdmin & Scope, Boolean] =
    ZIO.serviceWithZIO[PravegaAdmin](
      _.dropReaderGroup(scope, readerGroupName)
    )

  def createScope(scope: String): RIO[PravegaAdmin & Scope, Boolean] =
    ZIO.serviceWithZIO[PravegaAdmin](_.createScope(scope))

  def dropScope(scope: String): RIO[PravegaAdmin & Scope, Boolean] =
    ZIO.serviceWithZIO[PravegaAdmin](_.dropScope(scope))

  def createStream(
      scope: String,
      streamName: String,
      config: StreamConfiguration
  ): RIO[PravegaAdmin & Scope, Boolean] =
    ZIO.serviceWithZIO[PravegaAdmin](
      _.createStream(scope, streamName, config)
    )

  def sealStream(
      scope: String,
      streamName: String
  ): RIO[PravegaAdmin & Scope, Boolean] =
    ZIO.serviceWithZIO[PravegaAdmin](_.sealStream(scope, streamName))

  def dropStream(
      scope: String,
      streamName: String
  ): RIO[PravegaAdmin & Scope, Boolean] =
    ZIO.serviceWithZIO[PravegaAdmin](_.dropStream(scope, streamName))

  def createTable(
      scope: String,
      tableName: String,
      config: KeyValueTableConfiguration
  ): RIO[PravegaAdmin & Scope, Boolean] =
    ZIO.serviceWithZIO[PravegaAdmin](
      _.createTable(scope, tableName, config)
    )

  def dropTable(
      scope: String,
      tableName: String
  ): RIO[PravegaAdmin & Scope, Boolean] =
    ZIO.serviceWithZIO[PravegaAdmin](_.dropTable(scope, tableName))

  def readerOffline(
      scope: String,
      groupName: String
  ): RIO[PravegaAdmin & Scope, Int] =
    ZIO.serviceWithZIO[PravegaAdmin](_.readerOffline(scope, groupName))

  def live(
      clientConfig: ClientConfig
  ): ZLayer[Any, Nothing, PravegaAdmin] =
    ZLayer.succeed(new PravegaAdminImpl(clientConfig))

}

private class PravegaAdminImpl(clientConfig: ClientConfig)
    extends PravegaAdmin {

  override def sealStream(
      scope: String,
      streamName: String
  ): RIO[Scope, Boolean] = for {
    streamManager <- streamManager()
    _ <- ZIO.attemptBlocking(streamManager.sealStream(scope, streamName))
  } yield true

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

  def dropReaderGroup(
      scope: String,
      readerGroupName: String
  ): RIO[Scope, Boolean] = for {
    manager <- readerGroupManager(scope)
    _ <- ZIO.attemptBlocking(manager.deleteReaderGroup(readerGroupName))
  } yield true

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

  def dropScope(scope: String): RIO[Scope, Boolean] = for {
    streamManager <- streamManager()
    dropped <- ZIO.attemptBlocking(streamManager.deleteScope(scope))
  } yield dropped

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

  override def dropStream(
      scope: String,
      streamName: String
  ): RIO[Scope, Boolean] = for {
    streamManager <- streamManager()
    dropped <- ZIO.attemptBlocking(
      streamManager.deleteStream(scope, streamName)
    )
  } yield dropped

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
      scope: String,
      tableName: String,
      config: KeyValueTableConfiguration
  ): RIO[Scope, Boolean] =
    for {
      keyValueTableManager <- keyValueTableManager()
      created <- ZIO.attemptBlocking(
        keyValueTableManager.createKeyValueTable(scope, tableName, config)
      )
    } yield created

  override def dropTable(
      scope: String,
      tableName: String
  ): RIO[Scope, Boolean] = for {
    keyValueTableManager <- keyValueTableManager()
    deleted <- ZIO.attemptBlocking(
      keyValueTableManager.deleteKeyValueTable(scope, tableName)
    )
  } yield deleted

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
