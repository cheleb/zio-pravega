package zio.pravega

import zio._
import zio.test._
import zio.test.TestAspect._
import zio.test.Assertion._
import zio.pravega.test.PravegaContainer
import io.pravega.client.stream.StreamConfiguration
import io.pravega.client.stream.ScalingPolicy

object PravegaAdminSpec extends ZIOSpec[PravegaAdminService] {

  val layer: ZLayer[Scope, Nothing, PravegaAdminService] =
    (PravegaContainer.pravega >>> PravegaContainer.clientConfig >>> PravegaAdmin.layer)

  def spec =
    suite("Pravega Admin")(
      test("Scope created once")(
        PravegaAdminService(_.createScope("scope-a"))
          .map(once => assert(once)(isTrue))
      ),
      test("Scope skip twice")(
        PravegaAdminService(_.createScope("scope-a"))
          .map(twice => assert(twice)(isFalse))
      ),
      test("Stream created once")(
        PravegaAdminService(
          _.createStream(
            "stream-a",
            StreamConfiguration.builder
              .scalingPolicy(ScalingPolicy.fixed(8))
              .build,
            "scope-a"
          )
        )
          .map(once => assert(once)(isTrue))
      ),
      test("Stream creation skiped")(
        PravegaAdminService(
          _.createStream(
            "stream-a",
            StreamConfiguration.builder
              .scalingPolicy(ScalingPolicy.fixed(8))
              .build,
            "scope-a"
          )
        )
          .map(twice => assert(twice)(isFalse))
      )
    ) @@ sequential

}
