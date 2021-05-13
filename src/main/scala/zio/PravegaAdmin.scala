package zio

import io.pravega.client.ClientConfig
import io.pravega.client.admin.StreamManager
import io.pravega.client.admin.ReaderGroupManager
import java.net.URI

import scala.jdk.CollectionConverters._

object PravegaAdmin {

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

  def streamManager(clientConfig: ClientConfig): ZManaged[Has[Console], Throwable, StreamManager] =
    ZIO.attemptBlocking(StreamManager.create(clientConfig)).toManagedAuto

  def readerOffline(groupName: String): ZIO[Has[ReaderGroupManager], Throwable, Int] =
    for {
      groupManager <- ZIO.service[ReaderGroupManager]
      freed <- ZIO.attemptBlocking(groupManager.getReaderGroup(groupName)).toManagedAuto.use { group =>
                ZIO.foreach(group.getOnlineReaders().asScala.toSeq)(id => ZIO(group.readerOffline(id, null)))
              }
    } yield freed.size

}
