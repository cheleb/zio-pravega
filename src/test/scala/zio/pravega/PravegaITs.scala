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
  val pravegaStreamName = "zio-stream"
  val pravegaTableName = "ziotable"

  val layer: ZLayer[Scope, TestFailure[
    Nothing
  ], PravegaAdmin & PravegaStreamService & PravegaTableService] =
    PravegaContainer.pravega >>> PravegaContainer.clientConfig >>> (PravegaAdmin.layer ++
      PravegaStreamLayer
        .fromScope(pravegaScope)
        .mapError(t => TestFailure.die(t)) ++
      PravegaTableLayer
        .fromScope(pravegaScope)
        .mapError(t => TestFailure.die(t)))

  val groupName = "coco1"

  def spec =
    suite("Pravega")(
      adminSuite(pravegaScope, pravegaStreamName),
      streamSuite(pravegaScope, pravegaStreamName, groupName),
      adminSuite2(pravegaScope, pravegaTableName),
      tableSuite(pravegaTableName)
    ) @@ sequential

}
