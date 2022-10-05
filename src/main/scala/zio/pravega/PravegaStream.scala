package zio.pravega

import io.pravega.client.ClientConfig
import io.pravega.client.EventStreamClientFactory

import zio._

import zio.stream._

import java.util.UUID
import zio.Exit.Failure
import zio.Exit.Success
import io.pravega.client.stream.EventRead

/** Pravega Stream API.
  */
trait PravegaStream {
  def sink[A](
      streamName: String,
      settings: WriterSettings[A]
  ): Task[ZSink[Any, Throwable, A, Nothing, Unit]]

  def sinkTx[A](
      streamName: String,
      settings: WriterSettings[A]
  ): Task[ZSink[Any, Throwable, A, Nothing, Unit]]

  def stream[A](
      readerGroupName: String,
      settings: ReaderSettings[A]
  ): Task[ZStream[Any, Throwable, A]]

  def eventStream[A](
      readerGroupName: String,
      settings: ReaderSettings[A]
  ): Task[ZStream[Any, Throwable, EventRead[A]]]
}

private class PravegaStreamImpl(
    eventStreamClientFactory: EventStreamClientFactory
) extends PravegaStream {

  override def sink[A](
      streamName: String,
      settings: WriterSettings[A]
  ): Task[ZSink[Any, Throwable, A, Nothing, Unit]] = {
    val acquireWriter = ZIO
      .attemptBlocking(
        eventStreamClientFactory.createEventWriter(
          streamName,
          settings.serializer,
          settings.eventWriterConfig
        )
      )
      .withFinalizerAuto
      .map { writer =>
        val writeEvent: A => Task[Void] = settings.keyExtractor match {
          case None =>
            a => ZIO.fromCompletableFuture(writer.writeEvent(a))
          case Some(extractor) =>
            a => ZIO.fromCompletableFuture(writer.writeEvent(extractor(a), a))
        }
        ZSink.foreach(writeEvent)
      }

    ZIO.attempt(
      ZSink.unwrapScoped(acquireWriter)
    )

  }

  def sinkTx[A](
      streamName: String,
      settings: WriterSettings[A]
  ): Task[ZSink[Any, Throwable, A, Nothing, Unit]] = {
    val acquireWriter = ZIO
      .attemptBlocking(
        eventStreamClientFactory.createTransactionalEventWriter(
          streamName,
          settings.serializer,
          settings.eventWriterConfig
        )
      )
      .withFinalizerAuto
      .flatMap(wtx =>
        ZIO.acquireReleaseExit(ZIO.attemptBlocking(wtx.beginTxn)) {
          case (tx, exit) =>
            exit match {
              case Failure(e) =>
                ZIO.logCause(e) *>
                  ZIO
                    .attemptBlocking(tx.abort())
                    .orDie
              case Success(_) =>
                ZIO.attemptBlocking(tx.commit()).orDie
            }
        }
      )
      .map { writer =>
        val writeEvent: A => Task[Unit] = settings.keyExtractor match {
          case None =>
            a => ZIO.attemptBlocking(writer.writeEvent(a))
          case Some(extractor) =>
            a => ZIO.attemptBlocking(writer.writeEvent(extractor(a), a))
        }
        ZSink.foreach(writeEvent)
      }

    ZIO.attempt(
      ZSink.unwrapScoped(acquireWriter)
    )

  }

  override def stream[A](
      readerGroupName: String,
      settings: ReaderSettings[A]
  ): Task[ZStream[Any, Throwable, A]] = ZIO.attemptBlocking(
    ZStream.unwrapScoped(
      ZIO
        .attemptBlocking(
          eventStreamClientFactory
            .createReader(
              settings.readerId.getOrElse(UUID.randomUUID().toString),
              readerGroupName,
              settings.serializer,
              settings.readerConfig
            )
        )
        .withFinalizerAuto
        .map(reader =>
          ZStream.repeatZIOChunk(
            ZIO.attemptBlocking(reader.readNextEvent(settings.timeout) match {
              case eventRead if eventRead.isCheckpoint => Chunk.empty
              case eventRead =>
                val event = eventRead.getEvent()
                if (event == null) Chunk.empty
                else Chunk.single(event)
            })
          )
        )
    )
  )

  override def eventStream[A](
      readerGroupName: String,
      settings: ReaderSettings[A]
  ): Task[ZStream[Any, Throwable, EventRead[A]]] = ZIO.attempt(
    ZStream.unwrapScoped(
      ZIO
        .attemptBlocking(
          eventStreamClientFactory
            .createReader(
              settings.readerId.getOrElse(UUID.randomUUID().toString),
              readerGroupName,
              settings.serializer,
              settings.readerConfig
            )
        )
        .withFinalizerAuto
        .map(reader =>
          ZStream.repeatZIOChunk(
            ZIO.attemptBlocking(reader.readNextEvent(settings.timeout) match {
              case eventRead if eventRead.isCheckpoint =>
                Chunk.single(eventRead)
              case eventRead if eventRead.getEvent() == null => Chunk.empty
              case eventRead => Chunk.single(eventRead)
            })
          )
        )
    )
  )

}

object PravegaStream {

  def sink[A](
      streamName: String,
      settings: WriterSettings[A]
  ): RIO[PravegaStream, ZSink[Any, Throwable, A, Nothing, Unit]] =
    ZIO.serviceWithZIO[PravegaStream](_.sink(streamName, settings))

  def sinkTx[A](
      streamName: String,
      settings: WriterSettings[A]
  ): RIO[PravegaStream, ZSink[Any, Throwable, A, Nothing, Unit]] =
    ZIO.serviceWithZIO[PravegaStream](_.sinkTx(streamName, settings))

  def stream[A](
      readerGroupName: String,
      settings: ReaderSettings[A]
  ): RIO[PravegaStream, ZStream[Any, Throwable, A]] =
    ZIO.serviceWithZIO[PravegaStream](
      _.stream(readerGroupName, settings)
    )
  def eventStream[A](
      readerGroupName: String,
      settings: ReaderSettings[A]
  ): RIO[PravegaStream, ZStream[Any, Throwable, EventRead[A]]] =
    ZIO.serviceWithZIO[PravegaStream](
      _.eventStream(readerGroupName, settings)
    )

  private def streamService(
      scope: String,
      clientConfig: ClientConfig
  ): ZIO[Scope, Throwable, PravegaStream] =
    ZIO
      .attemptBlocking(
        EventStreamClientFactory.withScope(scope, clientConfig)
      )
      .withFinalizerAuto
      .map(new PravegaStreamImpl(_))

  def fromScope(
      scope: String,
      clientConfig: ClientConfig
  ): ZLayer[Scope, Throwable, PravegaStream] = ZLayer(
    streamService(scope, clientConfig)
  )

}
