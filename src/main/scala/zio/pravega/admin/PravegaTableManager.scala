package zio.pravega

import zio._

import io.pravega.client.ClientConfig

import io.pravega.client.tables.KeyValueTableConfiguration
import io.pravega.client.admin.KeyValueTableManager

/**
 * Pravega Admin API for KeyValueTable.
 */
trait PravegaTableManager {

  /**
   * Create a KeyValueTable with the given name and config, may block.
   *
   * Will return false if the KeyValueTable already exists, hence true if it was
   * effectively created.
   */
  def createTable(scope: String, tableName: String, config: KeyValueTableConfiguration): RIO[Scope, Boolean]

  /**
   * Delete a KeyValueTable with the given name, may block.
   *
   * Will return false if the KeyValueTable does not exist, hence true if it was
   * effectively dropped.
   */
  def deleteTable(scope: String, tableName: String): RIO[Scope, Boolean]

}

/**
 * Pravega Admin API implementation for KeyValueTable.
 */
final private case class PravegaTableManagerLive(keyValueTableManager: KeyValueTableManager)
    extends PravegaTableManager {

  /**
   * Create a KeyValueTable with the given name and config, may block.
   *
   * Will return false if the KeyValueTable already exists, hence true if it was
   * effectively created.
   */
  override def createTable(scope: String, tableName: String, config: KeyValueTableConfiguration): RIO[Scope, Boolean] =
    for {
      created <- ZIO.attemptBlocking(keyValueTableManager.createKeyValueTable(scope, tableName, config))
    } yield created

  /**
   * Delete a KeyValueTable with the given name, may block.
   *
   * Will return false if the KeyValueTable does not exist, hence true if it was
   * effectively dropped.
   */
  override def deleteTable(scope: String, tableName: String): RIO[Scope, Boolean] = for {
    deleted <- ZIO.attemptBlocking(keyValueTableManager.deleteKeyValueTable(scope, tableName))
  } yield deleted

}

/**
 * Pravega Admin API for KeyValueTable.
 */
object PravegaTableManager {

  /**
   * Create a KeyValueTable with the given name and config, may block.
   *
   * Will return false if the KeyValueTable already exists, hence true if it was
   * effectively created.
   */
  def createTable(
    scope: String,
    tableName: String,
    config: KeyValueTableConfiguration
  ): RIO[PravegaTableManager & Scope, Boolean] =
    ZIO.serviceWithZIO[PravegaTableManager](_.createTable(scope, tableName, config))

  /**
   * Delete a KeyValueTable with the given name, may block.
   *
   * Will return false if the KeyValueTable does not exist, hence true if it was
   * effectively dropped.
   */
  def deleteTable(scope: String, tableName: String): RIO[PravegaTableManager & Scope, Boolean] = ZIO
    .serviceWithZIO[PravegaTableManager](_.deleteTable(scope, tableName))

  /** Create a PravegaTableManager from a ClientConfig. */
  private def keyValueTableManager(clientConfig: ClientConfig) = ZIO
    .attemptBlocking(KeyValueTableManager.create(clientConfig))
    .withFinalizerAuto
    .map(keyValueTableManager => PravegaTableManagerLive(keyValueTableManager))

  /**
   * ZLayer to provide a PravegaTableManager from a ClientConfig.
   */
  def live(
    clientConfig: ClientConfig
  ): RLayer[Scope, PravegaTableManager] =
    ZLayer.fromZIO(keyValueTableManager(clientConfig))

  /**
   * ZLayer to provide a PravegaTableManager from a environment with a
   * ClientConfig.
   */
  val live: RLayer[Scope & ClientConfig, PravegaTableManager] =
    ZLayer.fromZIO(
      ZIO.serviceWithZIO[ClientConfig](keyValueTableManager)
    )

}
