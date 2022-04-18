package zio.pravega

import io.pravega.client.tables.TableKey
import io.pravega.client.tables.Version

final case class TableEntry[+V](tableKey: TableKey, version: Version, value: V)
