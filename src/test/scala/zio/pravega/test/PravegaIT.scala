package zio.pravega.test

import zio._
import zio.test._
import zio.test.RunnableSpec
//import zio.test.environment._

import zio.test.TestAspect
import zio.test.TestRunner
import zio.test.TestExecutor

object ITSpec {
  type ITEnv =
    TestEnvironment with PravegaContainer // with Logging with Postgres
}
import ITSpec.ITEnv

abstract class PravegaIT extends RunnableSpec[ITEnv, Any] {

  type ITSpec = ZSpec[ITEnv, Any]

  override def aspects: List[TestAspectAtLeastR[Environment]] =
    List(TestAspect.timeout(60.seconds))

  override def runner: TestRunner[ITEnv, Any] =
    TestRunner(TestExecutor.default(itLayer))

  val pravega = TestContainer.pravega(
    sys.env.getOrElse("PRAVEGA_IMAGE", "pravega/pravega:0.10.1")
  )

  val itLayer: Layer[Nothing, ITEnv] =
    testEnvironment ++ pravega
}
