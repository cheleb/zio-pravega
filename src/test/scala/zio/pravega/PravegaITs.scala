package zio.pravega

import zio.pravega._

//import zio.pravega.test.PravegaContainer

object PravegaITs {
//    with AdminSpec
//    with StreamSpec

  val clientConfig = PravegaClientConfig.default

  val pravegaScope = "zio-scope"

  // val bootstrap =
  //   PravegaContainer.pravega ++ PravegaAdmin.live(clientConfig) ++
  //     PravegaStream
  //       .fromScope(pravegaScope, clientConfig) ++
  //     PravegaTable
  //       .fromScope(pravegaScope, clientConfig)

  // adminSuite(
  // pravegaScope,
  // pravegaStreamName1,
  // pravegaStreamName2,
  // groupName1,
  // groupName2
  // ),
//      streamSuite(pravegaStreamName1, groupName1),
//TODO      eventStreamSuite(pravegaStreamName2, groupName2),
//TODO      adminSuite2(pravegaScope, pravegaTableName),
//TODO      tableSuite(pravegaTableName),
//TODO      streamAndTable(pravegaScope, pravegaStreamName1)
//TODO      adminCleanSpec
  //   ) @@ sequential
}
