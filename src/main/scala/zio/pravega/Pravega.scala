package zio.pravega

import zio._
import zio.stream._
import zio.Accessible
import io.pravega.client.ClientConfig
import io.pravega.client.stream.EventStreamReader
import java.util.UUID
import io.pravega.client.EventStreamClientFactory

trait PravegaService {
  def pravegaSink[A](
      streamName: String,
      settings: WriterSettings[A]
  ): Task[ZSink[Any, Throwable, A, Throwable, Nothing, Unit]]
  def pravegaStream[A](
      readerGroupName: String,
      settings: ReaderSettings[A]
  ): Task[ZStream[Any, Throwable, A]]
}

object PravegaService extends Accessible[PravegaService]

case class Pravega(eventStreamClientFactory: EventStreamClientFactory)
    extends PravegaService {

  override def pravegaSink[A](
      streamName: String,
      settings: WriterSettings[A]
  ): Task[ZSink[Any, Throwable, A, Throwable, Nothing, Unit]] = {
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
        ZSink.foreach((a: A) => ZIO.fromCompletableFuture(writer.writeEvent(a)))
      )

    Task(
      ZSink
        .unwrapManaged(writerManaged)
    )

  }

  override def pravegaStream[A](
      readerGroupName: String,
      settings: ReaderSettings[A]
  ): Task[ZStream[Any, Throwable, A]] = {
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

    Task(
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
    )

  }

}

object Pravega {
  def apply(eventStreamClientFactory: EventStreamClientFactory) = new Pravega(
    eventStreamClientFactory
  )
  def layer(
      scope: String,
      clientConfig: ClientConfig
  ): ZLayer[Any, Throwable, PravegaService] = ZIO
    .attempt(EventStreamClientFactory.withScope(scope, clientConfig))
    .map(eventStreamClientFactory => Pravega(eventStreamClientFactory))
    .toManagedWith(pravega => UIO(pravega.eventStreamClientFactory.close()))
    .toLayer
}
