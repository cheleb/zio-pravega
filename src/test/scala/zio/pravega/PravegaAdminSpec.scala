package zio.pravega

import zio.pravega.test.PravegaIT

import zio.test._
import zio.test.Assertion._

object PravegaAdminSpec extends PravegaIT {

  val clientConfig = PravegaClientConfigBuilder().build()

  val program = for {
    first <- PravegaAdminService(_.createScope("scope-a"))
    second <- PravegaAdminService(_.createScope("scope-a"))
  } yield first && !second
  override def spec =
    suite("Pravega Admin")(
      zio.test.test("Scope created once")(
        for {
          first <- program.provideCustomLayer(
            PravegaAdmin.layer(
              clientConfig
            )
          )

        } yield assert(first)(isTrue)
      )
    )

}
