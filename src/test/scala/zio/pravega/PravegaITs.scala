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
    with TableSpecs {
  val pravegaScope = "zio-scope"

  val layer: ZLayer[Scope, TestFailure[
    Nothing
  ], PravegaAdmin & PravegaStreamService & PravegaTableService] =
    PravegaContainer.pravega >>> PravegaContainer.clientConfig >>> (PravegaAdminLayer.layer ++
      PravegaStreamLayer
        .fromScope(pravegaScope)
        .mapError(t => TestFailure.die(t)) ++
      PravegaTableLayer
        .fromScope(pravegaScope)
        .mapError(t => TestFailure.die(t)))

  def spec = {

    val pravegaStreamName = "zio-stream"
    val pravegaTableName = "ziotable"

    val groupName = "coco1"

    suite("Pravega")(
      adminSuite(pravegaScope, pravegaStreamName, groupName),
      streamSuite(pravegaStreamName, groupName),
      adminSuite2(pravegaScope, pravegaTableName),
      tableSuite(pravegaTableName),
      adminCleanSpec
    ) @@ sequential
  }
}
