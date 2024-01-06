package zio.pravega.docs

import zio._
import zio.Console._
import zio.pravega.PravegaClientConfig
import zio.pravega.admin._

import io.pravega.client.stream.StreamConfiguration
import io.pravega.client.stream.ScalingPolicy
import zio.pravega.PravegaTableManager
import io.pravega.client.tables.KeyValueTableConfiguration

object CreateResourcesExample extends ZIOAppDefault {

  private val streamConfiguration = StreamConfiguration.builder.scalingPolicy(ScalingPolicy.fixed(1)).build

  private val scopeName             = "a-scope"
  private val streamName            = "a-stream"
  private val tableName             = "a-table"
  private val createReaderGroupName = "a-reader-group"

  val tableConfig = KeyValueTableConfiguration
    .builder()
    .partitionCount(2)
    .primaryKeyLength(4)
    .build()

  private val program = for {
    scopeCreated  <- PravegaStreamManager.createScope(scopeName)
    _             <- printLine(f"Scope created: $scopeCreated%b")
    streamCreated <- PravegaStreamManager.createStream(scopeName, streamName, streamConfiguration)
    _             <- printLine(f"Stream created: $streamCreated%b")
    _             <- PravegaReaderGroupManager.createReaderGroup(createReaderGroupName, streamName)

    tableCreated <- PravegaTableManager.createTable(
                      scopeName,
                      tableName,
                      tableConfig
                    )
    _ <- printLine(f"Table created: $tableCreated%b")
  } yield ()

  override def run: ZIO[Scope, Throwable, Unit] = program
    .provideSome[Scope](
      PravegaClientConfig.live,
      PravegaReaderGroupManager.live("a-scope"),
      PravegaTableManager.live,
      PravegaStreamManager.live
    )

}
