package zio.pravega.stream

import zio._
import zio.pravega.WriterSettings
import io.pravega.client.stream.EventStreamWriter
import io.pravega.client.stream.Transaction

object EventWriter {
  def writeEventTask[A](writer: EventStreamWriter[A], settings: WriterSettings[A]): A => Task[Void] =
    settings.keyExtractor match {
      case None            => a => ZIO.fromCompletableFuture(writer.writeEvent(a))
      case Some(extractor) => a => ZIO.fromCompletableFuture(writer.writeEvent(extractor(a), a))
    }

  def writeEventTask[A](tx: Transaction[A], settings: WriterSettings[A]): A => Task[Unit] =
    settings.keyExtractor match {
      case None            => a => ZIO.attemptBlocking(tx.writeEvent(a))
      case Some(extractor) => a => ZIO.attemptBlocking(tx.writeEvent(extractor(a), a))
    }

}
