package zio

import io.pravega.client.ClientConfig
import io.pravega.client.admin.StreamManager
import io.pravega.client.admin.ReaderGroupManager
import java.net.URI

import scala.jdk.CollectionConverters._
import io.pravega.client.stream.StreamConfiguration
import zio.pravega.ReaderSettings
import io.pravega.client.stream.ReaderGroupConfig
import io.pravega.client.stream.Stream

object PravegaAdmin {

  def readerGroup[A](
      scope: String,
      readerGroupName: String,
      readerSettings: ReaderSettings[A],
      streamNames: String*
  ): ZIO[Any, Throwable, Boolean] = {
    def config() = {
      val builder = ReaderGroupConfig.builder()
      streamNames.foreach(name => builder.stream(Stream.of(scope, name)))
      builder.build()
    }

    PravegaAdmin.readerGroupManager(scope, readerSettings.clientConfig).use {
      manager =>
        ZIO.attemptBlocking {
          manager.createReaderGroup(
            readerGroupName,
            config()
          )
        }
    }

  }

  def createScope(scope: String): ZIO[Has[StreamManager], Throwable, Boolean] =
    for {
      manager <- ZIO.service[StreamManager]
      exists <- ZIO.attemptBlocking(manager.checkScopeExists(scope))
      created <- exists match {
        case true  => ZIO.succeed(false)
        case false => ZIO.attemptBlocking(manager.createScope(scope))
      }
    } yield created

  def createStream(
      streamName: String,
      config: StreamConfiguration,
      scope: String
  ): ZIO[Has[StreamManager], Throwable, Boolean] =
    for {
      manager <- ZIO.service[StreamManager]
      exists <- ZIO.attemptBlocking(
        manager.checkStreamExists(scope, streamName)
      )
      created <- exists match {
        case true => ZIO.succeed(false)
        case false =>
          ZIO.attemptBlocking(manager.createStream(scope, streamName, config))
      }
    } yield created

  def readerGroupManager(
      scope: String,
      clientConfig: ClientConfig
  ): ZManaged[Any, Throwable, ReaderGroupManager] =
    ZIO(ReaderGroupManager.withScope(scope, clientConfig)).toManagedAuto

  def readerGroupManager(
      scope: String,
      controllerURI: URI
  ): ZManaged[Any, Throwable, ReaderGroupManager] =
    ZIO(ReaderGroupManager.withScope(scope, controllerURI)).toManagedAuto

  def streamManager(
      clientConfig: ClientConfig
  ): ZManaged[Has[Console], Throwable, StreamManager] =
    ZIO.attemptBlocking(StreamManager.create(clientConfig)).toManagedAuto

  def readerOffline(
      groupName: String
  ): ZIO[Has[ReaderGroupManager], Throwable, Int] =
    for {
      groupManager <- ZIO.service[ReaderGroupManager]
      freed <- ZIO
        .attemptBlocking(groupManager.getReaderGroup(groupName))
        .toManagedAuto
        .use { group =>
          ZIO.foreach(group.getOnlineReaders().asScala.toSeq)(id =>
            ZIO(group.readerOffline(id, null))
          )
        }
    } yield freed.size

}
