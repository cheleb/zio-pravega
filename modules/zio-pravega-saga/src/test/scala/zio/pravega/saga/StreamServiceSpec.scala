package zio.pravega.saga

import zio.*
import zio.test.*
import zio.stream.ZStream
import zio.pravega.PravegaStream

object StreamServiceSpec extends ZIOSpecDefault {
  def spec =
    suite("HelloWorldSpec")(
      test("Hello World") {
        for {

          _      <- ZIO.succeed(println("Hello World!"))
          service = new StreamService {
                      override def serviceLoop[R, Event, Error](eventStream: ZStream[PravegaStream, Error, Event])(
                        handler: Event => ZIO[R, Error, Unit]
                      ): ZIO[PravegaStream & R, Error, Unit] = ???
                    }
          _ <- service.test(42)
          _ <- ZIO.succeed(println("Hello World!"))

        } yield assertCompletes

      }
    )
}
