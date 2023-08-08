package zio.pravega

import io.pravega.client.ClientConfig
import io.pravega.client.EventStreamClientFactory
import io.pravega.client.stream.EventRead
import io.pravega.client.stream.TransactionalEventStreamWriter
import io.pravega.client.stream.Transaction
import io.pravega.client.stream.EventStreamReader

import java.util.UUID

import zio._
import zio.Exit.Failure
import zio.Exit.Success
import zio.pravega.stream.EventWriter
import zio.stream._

/**
 * Pravega Stream API.
 */
@Accessible
trait PravegaStream {
  def sink[A](streamName: String, settings: WriterSettings[A]): Sink[Throwable, A, Nothing, Unit]

  def sinkTx[A](streamName: String, settings: WriterSettings[A]): Sink[Throwable, A, Nothing, Unit]

  def writeFlow[A](streamName: String, settings: WriterSettings[A]): ZPipeline[Any, Throwable, A, A]

  def stream[A](readerGroupName: String, settings: ReaderSettings[A]): Stream[Throwable, A]

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  def eventStream[A](readerGroupName: String, settings: ReaderSettings[A]): Stream[Throwable, EventRead[A]]
}

private class PravegaStreamImpl(eventStreamClientFactory: EventStreamClientFactory) extends PravegaStream {
  private def createEventWriter[A](streamName: String, settings: WriterSettings[A]) = ZIO
    .attemptBlocking(
      eventStreamClientFactory.createEventWriter(streamName, settings.serializer, settings.eventWriterConfig)
    )
    .withFinalizerAuto
  private def createEventStreamReader[A](readerGroupName: String, settings: ReaderSettings[A]) = ZIO
    .attemptBlocking(
      eventStreamClientFactory.createReader(
        settings.readerId.getOrElse(UUID.randomUUID().toString),
        readerGroupName,
        settings.serializer,
        settings.readerConfig
      )
    )
    .withFinalizerAuto
  def sink[A](streamName: String, settings: WriterSettings[A]): Sink[Throwable, A, Nothing, Unit] = ZSink.unwrapScoped(
    for (writer <- createEventWriter(streamName, settings); eventWriter = EventWriter.writeEventTask(writer, settings))
      yield ZSink.foreach(eventWriter)
  )
  def writeFlow[A](streamName: String, settings: WriterSettings[A]): ZPipeline[Any, Throwable, A, A] =
    ZPipeline.unwrapScoped {
      for (
        writer <- createEventWriter(streamName, settings); eventWriter = EventWriter.writeEventTask(writer, settings)
      ) yield ZPipeline.tap(eventWriter)
    }
  private def createTxEventWriter[A](streamName: String, settings: WriterSettings[A]) = ZIO
    .attemptBlocking(
      eventStreamClientFactory
        .createTransactionalEventWriter(streamName, settings.serializer, settings.eventWriterConfig)
    )
    .withFinalizerAuto
  private def beginTransaction[A](writer: TransactionalEventStreamWriter[A]): RIO[Scope, Transaction[A]] =
    ZIO.acquireReleaseExit(ZIO.attemptBlocking(writer.beginTxn)) {
      case (tx, Failure(e)) =>
        ZIO.logCause(e) *> ZIO.attemptBlocking(tx.abort()).orDie
      case (tx, Success(_)) =>
        ZIO.attemptBlocking(tx.commit()).orDie

    }
  def sinkTx[A](streamName: String, settings: WriterSettings[A]): Sink[Throwable, A, Nothing, Unit] =
    ZSink.unwrapScoped(
      for (
        writer        <- createTxEventWriter(streamName, settings); tx <- beginTransaction(writer);
        writeEventTask = EventWriter.writeEventTask(tx, settings)
      ) yield ZSink.foreach(writeEventTask)
    )
  @SuppressWarnings(Array("org.wartremover.warts.Equals")) private def readNextEvent[A](
    reader: EventStreamReader[A],
    timeout: Long
  ): Task[Chunk[A]] = ZIO.attemptBlocking(reader.readNextEvent(timeout) match {
    case eventRead if eventRead.isCheckpoint =>
      Chunk.empty
    case eventRead =>
      val event = eventRead.getEvent()
      if (event == null) Chunk.empty else Chunk.single(event)
  })
  def stream[A](readerGroupName: String, settings: ReaderSettings[A]): Stream[Throwable, A] = ZStream.unwrapScoped(
    for (
      reader <- createEventStreamReader(readerGroupName, settings); readTask = readNextEvent(reader, settings.timeout)
    ) yield ZStream.repeatZIOChunk(readTask)
  )
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  def eventStream[A](readerGroupName: String, settings: ReaderSettings[A]): Stream[Throwable, EventRead[A]] =
    ZStream.unwrapScoped(
      createEventStreamReader(readerGroupName, settings).map(reader =>
        ZStream.repeatZIOChunk(ZIO.attemptBlocking(reader.readNextEvent(settings.timeout) match {
          case eventRead if eventRead.isCheckpoint =>
            Chunk.single(eventRead)
          case eventRead if eventRead.getEvent() == null =>
            Chunk.empty
          case eventRead =>
            Chunk.single(eventRead)
        }))
      )
    )
}

object PravegaStream {
  def sink[A](streamName: String, settings: WriterSettings[A]): ZSink[PravegaStream, Throwable, A, Nothing, Unit] =
    ZSink.serviceWithSink[PravegaStream](_.sink(streamName, settings))
  def sinkTx[A](streamName: String, settings: WriterSettings[A]): ZSink[PravegaStream, Throwable, A, Nothing, Unit] =
    ZSink.serviceWithSink[PravegaStream](_.sinkTx(streamName, settings))
  def stream[A](readerGroupName: String, settings: ReaderSettings[A]): ZStream[PravegaStream, Throwable, A] =
    ZStream.serviceWithStream[PravegaStream](_.stream(readerGroupName, settings))
  def eventStream[A](
    readerGroupName: String,
    settings: ReaderSettings[A]
  ): ZStream[PravegaStream, Throwable, EventRead[A]] =
    ZStream.serviceWithStream[PravegaStream](_.eventStream(readerGroupName, settings))
  private def streamService(scope: String, clientConfig: ClientConfig): ZIO[Scope, Throwable, PravegaStream] = ZIO
    .attemptBlocking(EventStreamClientFactory.withScope(scope, clientConfig))
    .withFinalizerAuto
    .map(new PravegaStreamImpl(_))
  def fromScope(scope: String): ZLayer[Scope & ClientConfig, Throwable, PravegaStream] =
    ZLayer.fromZIO(ZIO.serviceWithZIO[ClientConfig](streamService(scope, _)))
  def fromScope(scope: String, clientConfig: ClientConfig): ZLayer[Scope, Throwable, PravegaStream] = ZLayer(
    streamService(scope, clientConfig)
  )
  def writeFlow[A](streamName: String, settings: WriterSettings[A]): ZPipeline[PravegaStream, Throwable, A, A] =
    ZPipeline.serviceWithPipeline[PravegaStream](_.writeFlow(streamName, settings))
}
