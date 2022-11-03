package zio.pravega.table

import io.pravega.client.tables.Insert
import io.pravega.client.tables.TableEntry
import io.pravega.client.tables.Put
import io.pravega.client.tables.TableKey
import io.pravega.client.tables.KeyValueTable
import zio._
import io.pravega.client.tables.TableEntryUpdate
import zio.pravega.TableSettings

case class UpdateAndNewValue[V](update: TableEntryUpdate, newValue: V)
case class PravegaKeyValueTable[K, V](val table: KeyValueTable, settings: TableSettings[K, V]) {

  def updateTask(k: K, v: V, combine: (V, V) => V): Task[UpdateAndNewValue[V]] = ZIO
    .fromCompletableFuture(table.get(tableKey(k)))
    .map {
      case null     => insert(k, v)
      case previous => put(k, v, previous, combine)
    }

  def pushUpdate(updateAndNewValue: UpdateAndNewValue[V]) =
    ZIO
      .fromCompletableFuture(table.update(updateAndNewValue.update))
      .map(version => (version, updateAndNewValue.newValue))
      .tapError(o => ZIO.logDebug(o.toString))

  private def tableKey(key: K): TableKey = settings.tableKey(key)

  private def insert(key: K, value: V): UpdateAndNewValue[V] =
    UpdateAndNewValue(new Insert(settings.tableKey(key), settings.valueSerializer.serialize(value)), value)

  private def put(key: K, value: V, previous: TableEntry, combine: (V, V) => V): UpdateAndNewValue[V] = {
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
}
