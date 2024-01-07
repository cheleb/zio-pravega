package zio.pravega.stream

import zio.ZIO

final case class KillSwitch(kill: ZIO[Any, Nothing, Unit])
