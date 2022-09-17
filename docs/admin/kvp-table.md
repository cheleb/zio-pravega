---
sidebar_position: 3
---

# KVP Table

[Key Value Pair](https://github.com/pravega/pravega/wiki/PDP-48-(Key-Value-Tables-Beta-2)) tables are an early feature.

In the same way as [streams](stream.md) KVP table belong to a [scope](scope.md). 

It must be explictly created

```scala mdoc:silent
import zio._
import zio.Console._
import zio.pravega._
import io.pravega.client.tables.KeyValueTableConfiguration

val tableConfig = KeyValueTableConfiguration
    .builder()
    .partitionCount(2)
    .primaryKeyLength(4)
    .build()

def initTable(tableName: String, pravegaScope: String)
: ZIO[Scope & Console & PravegaAdmin,Throwable,Unit] =
    for {
      tableCreated <- PravegaAdmin.createTable(
              tableName,
              tableConfig,
              pravegaScope
            )
      
      _ <- ZIO.when(tableCreated)(
        printLine(s"Table $tableName just created")
      )
    } yield ()

````