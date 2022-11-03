package zio.pravega

import zio._

import io.pravega.client.ClientConfig

import io.pravega.client.tables.KeyValueTableConfiguration
import io.pravega.client.admin.KeyValueTableManager

/**
 * Pravega Admin API.
 */
trait PravegaTableManager {

  def createTable(scope: String, tableName: String, config: KeyValueTableConfiguration): RIO[Scope, Boolean]

  def dropTable(scope: String, tableName: String): RIO[Scope, Boolean]

}

private case class PravegaTableManagerImpl(clientConfig: ClientConfig) extends PravegaTableManager {

  def keyValueTableManager(): RIO[Scope, KeyValueTableManager] =
    ZIO.attemptBlocking(KeyValueTableManager.create(clientConfig)).withFinalizerAuto

  override def createTable(scope: String, tableName: String, config: KeyValueTableConfiguration): RIO[Scope, Boolean] =
    for {
      keyValueTableManager <- keyValueTableManager()
      created              <- ZIO.attemptBlocking(keyValueTableManager.createKeyValueTable(scope, tableName, config))
    } yield created

  override def dropTable(scope: String, tableName: String): RIO[Scope, Boolean] = for {
    keyValueTableManager <- keyValueTableManager()
    deleted              <- ZIO.attemptBlocking(keyValueTableManager.deleteKeyValueTable(scope, tableName))
  } yield deleted

}

object PravegaTableManager {

  def createTable(
    scope: String,
    tableName: String,
    config: KeyValueTableConfiguration
  ): RIO[PravegaTableManager & Scope, Boolean] =
    ZIO.serviceWithZIO[PravegaTableManager](_.createTable(scope, tableName, config))

  def dropTable(scope: String, tableName: String): RIO[PravegaTableManager & Scope, Boolean] = ZIO
    .serviceWithZIO[PravegaTableManager](_.dropTable(scope, tableName))

  def live(
    clientConfig: ClientConfig
  ): ZLayer[Any, Nothing, PravegaTableManager] =
    ZLayer.succeed(new PravegaTableManagerImpl(clientConfig))

  val live: ZLayer[ClientConfig, Nothing, PravegaTableManager] =
    ZLayer.fromFunction(PravegaTableManagerImpl(_))

}
