package zio.pravega

import zio._
import io.pravega.client.ClientConfig
import io.pravega.client.admin.StreamManager
import io.pravega.client.admin.ReaderGroupManager

import scala.jdk.CollectionConverters._
import io.pravega.client.stream.StreamConfiguration

import io.pravega.client.stream.ReaderGroupConfig
import io.pravega.client.stream.Stream

trait PravegaAdminService {
  def readerGroup[A](
      scope: String,
      readerGroupName: String,
      streamNames: String*
  ): ZIO[Any, Throwable, Boolean]

  def createScope(scope: String): ZIO[Any, Throwable, Boolean]

  def createStream(
      streamName: String,
      config: StreamConfiguration,
      scope: String
  ): ZIO[Any, Throwable, Boolean]

  def readerGroupManager(
      scope: String
  ): ZManaged[Any, Throwable, ReaderGroupManager]

  def readerGroupManager(
      scope: String,
      clientConfig: ClientConfig
  ): ZManaged[Any, Throwable, ReaderGroupManager]

  def streamManager(): ZManaged[Console, Throwable, StreamManager]

  def readerOffline(
      scope: String,
      groupName: String
  ): ZIO[Any, Throwable, Int]

}

object PravegaAdminService extends Accessible[PravegaAdminService]

case class PravegaAdmin(clientConfig: ClientConfig)
    extends PravegaAdminService {

  def readerGroup[A](
      scope: String,
      readerGroupName: String,
      streamNames: String*
  ): ZIO[Any, Throwable, Boolean] = {
    def config() = {
      val builder = ReaderGroupConfig.builder()
      streamNames.foreach(name => builder.stream(Stream.of(scope, name)))
      builder.build()
    }

    readerGroupManager(scope).use { manager =>
      ZIO.attemptBlocking {
        manager.createReaderGroup(
          readerGroupName,
          config()
        )
      }
    }

  }

  def createScope(scope: String): ZIO[Any, Throwable, Boolean] =
    streamManager().use(streamManager =>
      for {
        exists <- ZIO.attemptBlocking(streamManager.checkScopeExists(scope))
        created <- exists match {
          case true  => ZIO.succeed(false)
          case false => ZIO.attemptBlocking(streamManager.createScope(scope))
        }
      } yield created
    )

  def createStream(
      streamName: String,
      config: StreamConfiguration,
      scope: String
  ): ZIO[Any, Throwable, Boolean] =
    streamManager().use(streamManager =>
      for {

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
    )

  def readerGroupManager(
      scope: String
  ): ZManaged[Any, Throwable, ReaderGroupManager] =
    ZIO(ReaderGroupManager.withScope(scope, clientConfig)).toManagedAuto

  def readerGroupManager(
      scope: String,
      clientConfig: ClientConfig
  ): ZManaged[Any, Throwable, ReaderGroupManager] =
    ZIO(ReaderGroupManager.withScope(scope, clientConfig)).toManagedAuto

  def streamManager(): ZManaged[Any, Throwable, StreamManager] =
    ZIO.attemptBlocking(StreamManager.create(clientConfig)).toManagedAuto

  def readerOffline(
      scope: String,
      groupName: String
  ): ZIO[Any, Throwable, Int] =
    readerGroupManager(scope, clientConfig)
      .use(groupManager =>
        for {
          freed <- ZIO
            .attemptBlocking(groupManager.getReaderGroup(groupName))
            .toManagedAuto
            .use { group =>
              ZIO.foreach(group.getOnlineReaders().asScala.toSeq)(id =>
                ZIO(group.readerOffline(id, null))
              )
            }
        } yield freed.size
      )

}

object PravegaAdmin {
  def layer: ZLayer[ClientConfig, Nothing, PravegaAdmin] =
    (for {
      clientConfig <- ZIO.service[ClientConfig]
      l <- ZIO.succeed(new PravegaAdmin(clientConfig))

    } yield l).toLayer

}
