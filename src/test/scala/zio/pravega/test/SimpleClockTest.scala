package zio.pravega.test

import zio._
import zio.test.DefaultRunnableSpec
import zio.test.ZSpec
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._
import zio.test.environment.TestClock._

object SimpleClockTest extends DefaultRunnableSpec {

  def spec: ZSpec[Environment, Failure] =
    suite("ClockSpec")(
      test("sleep does not require passage of clock time") {
        for {
          ref    <- Ref.make(false)
          _      <- ref.set(true).delay(10.hours).fork
          _      <- adjust(11.hours)
          result <- ref.get
        } yield assert(result)(isTrue)
      } @@ forked @@ nonFlaky
    )

}
