package zio.pravega

import zio._
import io.pravega.client.tables.KeyValueTableConfiguration

object TestMe extends ZIOAppDefault {

  val tableConfig = KeyValueTableConfiguration
    .builder()
    .partitionCount(2)
    .primaryKeyLength(6)
    .build()

  override def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
    ZIO
      .scoped {
        for {
          _ <- PravegaAdmin.createScope("pravegaScope")
          _ <- PravegaAdmin.createTable(
            "pravegaTableName",
            tableConfig,
            "pravegaScope"
          )
        } yield ()
      }
      .provide(
        PravegaAdmin.live(PravegaClientConfig.default)
      )

}
