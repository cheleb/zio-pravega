package zio.pravega

import zio.test.TestAspect._

import zio._

import zio.test._

import zio.pravega._

import zio.pravega.test.PravegaContainer

object PravegaITs
    extends ZIOSpec[
      PravegaStreamService & PravegaAdminService & PravegaTableService
    ]
    with AdminSpec
    with StreamSpec
    with TableSpecs
    with StreamAndTableSpec {

  val clientConfig = PravegaClientConfigBuilder()
    .build()

  val pravegaScope = "zio-scope"

  val bootstrap =
    PravegaContainer.pravega ++ PravegaAdmin.live(clientConfig) ++
      PravegaStream
        .fromScope(pravegaScope, clientConfig) ++
      PravegaTable
        .fromScope(pravegaScope, clientConfig)

  def spec = {

    val pravegaStreamName = "zio-stream"
    val pravegaTableName = "ziotable"

    val groupName = "coco1"

    suite("Pravega")(
      adminSuite(pravegaScope, pravegaStreamName, groupName),
      streamSuite(pravegaStreamName, groupName),
      adminSuite2(pravegaScope, pravegaTableName),
      tableSuite(pravegaTableName),
      streamAndTable(pravegaScope, pravegaStreamName),
      adminCleanSpec
    ) @@ sequential
  }
}
