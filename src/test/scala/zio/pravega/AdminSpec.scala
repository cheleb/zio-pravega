package zio.pravega

import zio._
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._
import io.pravega.client.stream.StreamConfiguration
import io.pravega.client.stream.ScalingPolicy
import io.pravega.client.tables.KeyValueTableConfiguration

trait AdminSpec { this: ZIOSpec[_] =>

  val tableConfig = KeyValueTableConfiguration
    .builder()
    .partitionCount(2)
    .primaryKeyLength(4)
    .build()

  def adminSuite(
      pravegaScope: String,
      pravegaStreamName: String,
      groupName: String
  ): Spec[PravegaAdminService with Scope, TestFailure[Throwable], TestSuccess] =
    suite("Admin")(
      test("Scope created once")(
        PravegaAdmin(_.createScope(pravegaScope))
          .map(once => assert(once)(isTrue))
      ),
      test("Scope skip twice")(
        PravegaAdmin(_.createScope(pravegaScope))
          .map(twice => assert(twice)(isFalse))
      ),
      test("Stream created once")(
        PravegaAdmin(
          _.createStream(
            pravegaStreamName,
            StreamConfiguration.builder
              .scalingPolicy(ScalingPolicy.fixed(8))
              .build,
            pravegaScope
          )
        )
          .map(once => assert(once)(isTrue))
      ),
      test("Stream creation skiped")(
        PravegaAdmin(
          _.createStream(
            pravegaStreamName,
            StreamConfiguration.builder
              .scalingPolicy(ScalingPolicy.fixed(8))
              .build,
            pravegaScope
          )
        )
          .map(twice => assert(twice)(isFalse))
      ),
      test("Group")(
        PravegaAdmin(
          _.readerGroup(
            pravegaScope,
            groupName,
            pravegaStreamName
          )
        ).map(once => assert(once)(isTrue))
      )
    ) @@ sequential

  def adminSuite2(pravegaScope: String, pravegaTableName: String) =
    suite("Pravega KVP Table")(
      test(s"Create table $pravegaTableName")(
        PravegaAdmin(
          _.createTable(
            pravegaTableName,
            tableConfig,
            pravegaScope
          )
        )
          .map(created => assert(created)(isTrue))
      )
    )

  def adminCleanSpec =
    suite("Admin clean")(
      test("Clean reader")(
        PravegaAdmin(_.readerOffline("zio-scope", "coco1"))
          .map(n => assert(n)(equalTo(0)))
      )
    )

}