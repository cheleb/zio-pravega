# KVP Tables

KVP tables are a key-value pair abstraction on top of the underlying storage layer. They are used to store state in a streaming application.

The state is concurently updated in an optimistic way relying on the `version` field of the KVP table and retrying in case of conflict.

## Create a KVP table

```scala mdoc:silent
import zio.*
import zio.Console.*
import zio.pravega.*
import io.pravega.client.tables.KeyValueTableConfiguration

val tableConfig = KeyValueTableConfiguration
    .builder()
    .partitionCount(2)
    .primaryKeyLength(4)
    .build()

val createTable : RIO[Scope & PravegaTableManager, Boolean] =
        PravegaTableManager.createTable(
            "sales",
            "cumulative-sales-by-product-id",
            tableConfig
        )
```

`PravegaTableManager` is a service that provides table management operations (create, delete, list).

Very classical ZIO pattern, the service is defined as a trait and implemented as a layer, and the can be provided to the application.

```scala mdoc:silent
def run: RIO[Scope, Boolean] = createTable
    .provideSome[Scope](
      PravegaClientConfig.live,
      PravegaTableManager.live,
    )
```


