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

  val clientConfig = PravegaClientConfig.default

  val pravegaScope = "zio-scope"

  val bootstrap =
    PravegaContainer.pravega ++ PravegaAdmin.live(clientConfig) ++
      PravegaStream
        .fromScope(pravegaScope, clientConfig) ++
      PravegaTable
        .fromScope(pravegaScope, clientConfig)

  def spec = {

    val pravegaStreamName1 = "zio-stream"
    val pravegaStreamName2 = "zio-event-stream"
    val pravegaTableName = "ziotable"

    val groupName1 = "coco1"
    val groupName2 = "coco2"

    suite("Pravega")(
      adminSuite(
        pravegaScope,
        pravegaStreamName1,
        pravegaStreamName2,
        groupName1,
        groupName2
      ),
      streamSuite(pravegaStreamName1, groupName1),
      eventStreamSuite(pravegaStreamName2, groupName2),
      adminSuite2(pravegaScope, pravegaTableName),
      tableSuite(pravegaTableName),
      streamAndTable(pravegaScope, pravegaStreamName1),
      adminCleanSpec
    ) @@ sequential
  }
}
