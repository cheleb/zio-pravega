package zio.pravega.table

import io.pravega.client.tables.Insert
import io.pravega.client.tables.TableEntry
import io.pravega.client.tables.Put
import io.pravega.client.tables.TableKey
import io.pravega.client.tables.KeyValueTable
import zio._
import io.pravega.client.tables.TableEntryUpdate
import zio.pravega.TableSettings
import io.pravega.client.tables.Version

/**
 * Holds the update and the new value.
 *
 * Note: the new value is not necessarily the value that was received as input,
 * if the key value pair already existed in the table, the new value is the
 * result of the combine function.
 */
final case class UpdateAndNewValue[V](upsert: TableEntryUpdate, newValue: V)

/**
 * Pravega Table API.
 */
final case class PravegaKeyValueTable[K, V](val table: KeyValueTable, settings: TableSettings[K, V]) {

  /**
   * Build a command to Upsert a (key,value) pair in a Pravega table:
   *
   *   - if the key does not exist, build a command to insert the value.
   *   - if the key exists, build a command to update the value.
   *
   * The @combine function is used to merge old and new values.
   */
  def updateTask(k: K, v: V, combine: (V, V) => V): Task[UpdateAndNewValue[V]] = ZIO
    .fromCompletableFuture(table.get(tableKey(k)))
    .map {
      case null     => insert(k, v)
      case previous => merge(k, v, previous, combine)
    }

  /**
   * Build a command to Insert or override a (key,value) pair in a Pravega
   * table:
   *
   *   - if the key does not exist, build a command to insert the value.
   *   - if the key exists, build a command to override.
   */
  def overrideTask(k: K, v: V): Task[UpdateAndNewValue[V]] = ZIO
    .fromCompletableFuture(table.get(tableKey(k)))
    .map {
      case null => insert(k, v)
      case _    => put(k, v)
    }

  /**
   * Upsert a (key,value) pair in a Pravega table.
   *
   * In case of a conflict:
   *   - the key exists and the version does not match
   *   - the key did not exist at first lookup
   * the ZIO will fail and will be retried.
   */
  def pushUpdate(updateAndNewValue: UpdateAndNewValue[V]): ZIO[Any, Throwable, (Version, V)] =
    ZIO
      .fromCompletableFuture(table.update(updateAndNewValue.upsert))
      .map(version => (version, updateAndNewValue.newValue))
      .tapError(o => ZIO.logDebug(o.toString))

  /**
   * Convenience method to build a TableKey from the key.
   */
  private def tableKey(key: K): TableKey = settings.tableKey(key)

  /**
   * Convenience method to build an Insert command, and wrap it in an
   * UpdateAndNewValue.
   */
  private def insert(key: K, value: V): UpdateAndNewValue[V] =
    UpdateAndNewValue(new Insert(settings.tableKey(key), settings.valueSerializer.serialize(value)), value)

  /**
   * Convenience method to build a Put command, and wrap it in an
   * UpdateAndNewValue, along with the new value, that is the result of the
   * combine function on the old and new values.
   */
  private def merge(key: K, value: V, previous: TableEntry, combine: (V, V) => V): UpdateAndNewValue[V] = {
    val newValue = combine(settings.valueSerializer.deserialize(previous.getValue), value)
    UpdateAndNewValue(
      new Put(
        settings.tableKey(key),
        settings.valueSerializer.serialize(newValue),
        previous.getVersion
      ),
      newValue
    )
  }

  /**
   * Convenience method to build a Put command, and wrap it in an
   * UpdateAndNewValue, along with the new value, that is the result of the
   * combine function on the old and new values.
   */
  private def put(key: K, newValue: V): UpdateAndNewValue[V] =
    UpdateAndNewValue(
      new Put(
        settings.tableKey(key),
        settings.valueSerializer.serialize(newValue)
      ),
      newValue
    )

  def getEntry(key: K): Task[Option[TableEntry]] =
    ZIO
      .fromCompletableFuture(table.get(tableKey(key)))
      .map {
        case null => None
        case e    => Some(e)
      }
  def get(key: K): Task[Option[V]] =
    getEntry(key).map(_.map(e => settings.valueSerializer.deserialize(e.getValue)))
}
