package zio.pravega.stream

import zio.ZIO
import zio.stream.ZStream
import zio.pravega.PravegaStream

final case class InteruptibleStream[A](stream: ZStream[PravegaStream, Throwable, A], shutdown: ZIO[Any, Nothing, Unit])
