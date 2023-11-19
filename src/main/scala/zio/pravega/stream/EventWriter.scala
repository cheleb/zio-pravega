package zio.pravega.stream

import zio._
import zio.pravega.WriterSettings
import io.pravega.client.stream.EventStreamWriter
import io.pravega.client.stream.Transaction

/**
 * Pravega EventStreamWriter.
 *
 * This is a wrapper around the EventStreamWriter Java API.
 */
object EventWriter {

  /**
   * Build a lambda to produce a Task that write an event to a stream.
   */
  def writeEventTask[A](writer: EventStreamWriter[A], settings: WriterSettings[A]): A => Task[Void] =
    settings.keyExtractor match {
      case None            => a => ZIO.fromCompletableFuture(writer.writeEvent(a))
      case Some(extractor) => a => ZIO.fromCompletableFuture(writer.writeEvent(extractor(a), a))
    }

  /**
   * Build a lambda to produce a Task that write an event to a transaction.
   */
  def writeEventTask[A](tx: Transaction[A], settings: WriterSettings[A]): A => Task[Unit] =
    settings.keyExtractor match {
      case None            => a => ZIO.attemptBlocking(tx.writeEvent(a))
      case Some(extractor) => a => ZIO.attemptBlocking(tx.writeEvent(extractor(a), a))
    }

}
