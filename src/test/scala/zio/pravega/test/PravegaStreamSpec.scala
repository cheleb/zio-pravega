package zio.pravega.test

import zio.test._
import Assertion._

// Pravega docker container only works under linux
object PravegaStreamSpec extends /* PravegaIT */ DefaultRunnableSpec {

  val spec =
    suite("Prvavega")(
      test("publish and consume") {

        assertM(
          TestZioApp.program
        )(equalTo(10))
      }
    )

}
