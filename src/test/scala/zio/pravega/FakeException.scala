package zio.pravega

import scala.util.control.NoStackTrace

final case class FakeException(message: String) extends RuntimeException(message) with NoStackTrace
