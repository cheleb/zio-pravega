package zio.pravega.table

import zio.pravega.TableWriterSettings
import io.pravega.client.tables.Insert
import io.pravega.client.tables.TableEntry
import io.pravega.client.tables.Put
import io.pravega.client.tables.TableKey

class KVPWriter[K, V](settings: TableWriterSettings[K, V]) {

  def tableKey(key: K): TableKey = settings.tableKey(key)

  def insert(key: K, value: V): Insert = new Insert(
    settings.tableKey(key),
    settings.valueSerializer.serialize(value)
  )

  def put(key: K, value: V, previous: TableEntry, combine: (V, V) => V): Put =
    new Put(
      settings.tableKey(key),
      settings.valueSerializer
        .serialize(
          combine(
            settings.valueSerializer.deserialize(previous.getValue()),
            value
          )
        ),
      previous.getVersion
    )
}
