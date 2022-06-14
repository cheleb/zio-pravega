package zio.pravega

import io.pravega.client.ClientConfig
import io.pravega.client.EventStreamClientFactory

import zio._

import zio.stream._

import java.util.UUID

trait PravegaStreamService {
  def sink[A](
      streamName: String,
      settings: WriterSettings[A]
  ): Task[ZSink[Any, Throwable, A, Nothing, Unit]]
  def stream[A](
      readerGroupName: String,
      settings: ReaderSettings[A]
  ): Task[ZStream[Any, Throwable, A]]
}

private class PravegaStreamServiceImpl(
    eventStreamClientFactory: EventStreamClientFactory
) extends PravegaStreamService {

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
      .map(writer =>
        ZSink.foreach((a: A) => ZIO.fromCompletableFuture(writer.writeEvent(a)))
      )

    ZIO.attempt(
      ZSink.unwrapScoped(acquireWriter)
    )

  }

  override def stream[A](
      readerGroupName: String,
      settings: ReaderSettings[A]
  ): Task[ZStream[Any, Throwable, A]] = ZIO.attempt(
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

}

object PravegaStream {

  def sink[A](
      streamName: String,
      settings: WriterSettings[A]
  ): RIO[PravegaStreamService, ZSink[Any, Throwable, A, Nothing, Unit]] =
    ZIO.serviceWithZIO[PravegaStreamService](_.sink(streamName, settings))
  def stream[A](
      readerGroupName: String,
      settings: ReaderSettings[A]
  ): RIO[PravegaStreamService, ZStream[Any, Throwable, A]] =
    ZIO.serviceWithZIO[PravegaStreamService](
      _.stream(readerGroupName, settings)
    )

  private def streamService(
      scope: String,
      clientConfig: ClientConfig
  ): ZIO[Scope, Throwable, PravegaStreamService] =
    ZIO
      .attemptBlocking(
        EventStreamClientFactory.withScope(scope, clientConfig)
      )
      .withFinalizerAuto
      .map(new PravegaStreamServiceImpl(_))

  def fromScope(
      scope: String,
      clientConfig: ClientConfig
  ): ZLayer[Scope, Throwable, PravegaStreamService] = ZLayer(
    streamService(scope, clientConfig)
  )

}
