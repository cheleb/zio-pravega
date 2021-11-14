package zio

import io.pravega.client.ClientConfig
import io.pravega.client.EventStreamClientFactory

import io.pravega.client.stream.EventStreamReader

import java.util.UUID
import zio.stream.ZStream

import zio.stream.ZSink

import zio.pravega._

object Pravega {

  trait Service extends AutoCloseable {
    def pravegaSink[A](
        streamName: String,
        settings: WriterSettings[A]
    ): ZSink[Any, Throwable, A, Throwable, Nothing, Unit]
    def pravegaStream[A](
        readerGroupName: String,
        settings: ReaderSettings[A]
    ): ZStream[Any, Throwable, A]
  }

  object Service {

    def live(eventStreamClientFactory: EventStreamClientFactory) =
      new Service {

        override def pravegaStream[A](
            readerGroupName: String,
            settings: ReaderSettings[A]
        ): ZStream[Any, Throwable, A] = {
          case class QueuedReader(queue: Queue[A], reader: EventStreamReader[A])

          val reader = ZIO.attemptBlocking(
            eventStreamClientFactory
              .createReader(
                settings.readerId.getOrElse(UUID.randomUUID().toString),
                readerGroupName,
                settings.serializer,
                settings.readerConfig
              )
          )

          ZStream
            .acquireReleaseWith(reader)(reader =>
              ZIO.attemptBlocking(reader.close()).ignore
            )
            .flatMap(reader =>
              ZStream.repeatZIOChunk(
                ZIO(reader.readNextEvent(settings.timeout) match {
                  case eventRead if eventRead.isCheckpoint => Chunk.empty
                  case eventRead =>
                    val event = eventRead.getEvent()
                    if (event == null) Chunk.empty
                    else Chunk.single(event)
                })
              )
            )

        }

        override def pravegaSink[A](
            streamName: String,
            settings: WriterSettings[A]
        ): ZSink[Any, Throwable, A, Throwable, Nothing, Unit] = {
          val writerManaged = ZIO
            .attemptBlocking(
              eventStreamClientFactory.createEventWriter(
                streamName,
                settings.serializer,
                settings.eventWriterConfig
              )
            )
            .toManagedWith(w => ZIO.attemptBlocking(w.close()).ignore)
            .map(writer =>
              ZSink.foreach((a: A) =>
                ZIO.fromCompletableFuture(writer.writeEvent(a))
              )
            )

          ZSink
            .unwrapManaged(writerManaged)

        }

        override def close(): Unit =
          eventStreamClientFactory.close()
      }
  }

  def live(
      scope: String,
      clientConfig: ClientConfig
  ): ZServiceBuilder[Any, Throwable, Has[Service]] =
    ZIO
      .attempt(EventStreamClientFactory.withScope(scope, clientConfig))
      .map(eventStreamClientFactory =>
        Pravega.Service.live(eventStreamClientFactory)
      )
      .toManagedAuto
      .toServiceBuilder

  def pravegaSink[A](
      streamName: String,
      writterSettings: WriterSettings[A]
  ): ZIO[Has[Service], Throwable, ZSink[
    Any,
    Throwable,
    A,
    Throwable,
    Nothing,
    Unit
  ]] =
    ZIO.access(p => p.get.pravegaSink[A](streamName, writterSettings))

  def pravegaStream[A](
      readerGroup: String,
      readerSettings: ReaderSettings[A]
  ): ZIO[Has[Service], Throwable, ZStream[Has[Service], Throwable, A]] =
    ZIO.access[Has[Service]](p =>
      p.get.pravegaStream(readerGroup, readerSettings)
    )

}
