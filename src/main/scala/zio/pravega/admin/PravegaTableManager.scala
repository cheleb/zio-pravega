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

private case class PravegaTableManagerImpl(keyValueTableManager: KeyValueTableManager) extends PravegaTableManager {

  override def createTable(scope: String, tableName: String, config: KeyValueTableConfiguration): RIO[Scope, Boolean] =
    for {
      created <- ZIO.attemptBlocking(keyValueTableManager.createKeyValueTable(scope, tableName, config))
    } yield created

  override def dropTable(scope: String, tableName: String): RIO[Scope, Boolean] = for {
    deleted <- ZIO.attemptBlocking(keyValueTableManager.deleteKeyValueTable(scope, tableName))
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

  private def keyValueTableManager(clientConfig: ClientConfig) = ZIO
    .attemptBlocking(KeyValueTableManager.create(clientConfig))
    .withFinalizerAuto
    .map(keyValueTableManager => PravegaTableManagerImpl(keyValueTableManager))

  def live(
    clientConfig: ClientConfig
  ): RLayer[Scope, PravegaTableManager] =
    ZLayer.fromZIO(keyValueTableManager(clientConfig))

  val live: RLayer[Scope & ClientConfig, PravegaTableManager] =
    ZLayer.fromZIO(
      ZIO.serviceWithZIO[ClientConfig](keyValueTableManager)
    )

}
