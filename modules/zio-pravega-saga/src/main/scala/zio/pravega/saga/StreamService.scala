package zio.pravega.saga

import zio._
import zio.stream._
import zio.pravega._

/**
 * A stream service is a service that is implemented as a stream of events.
 */
trait StreamService {

  /**
   * A stream of commands.
   *
   * @param eventStream
   *   stream of events
   * @param handler
   *   function to execute the command
   * @return
   */
  def serviceLoop[R, Event, Error](eventStream: ZStream[PravegaStream, Error, Event])(
    handler: Event => ZIO[R, Error, Unit]
  ): ZIO[PravegaStream & R, Error, Unit] = eventStream
    .mapZIO(m => handler(m))
    .runDrain

}
