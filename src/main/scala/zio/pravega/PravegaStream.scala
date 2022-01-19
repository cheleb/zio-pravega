package zio.pravega

import io.pravega.client.ClientConfig
import io.pravega.client.EventStreamClientFactory
import zio.Accessible
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

object PravegaService extends Accessible[PravegaStreamService]

case class PravegaStream(eventStreamClientFactory: EventStreamClientFactory)
    extends PravegaStreamService {

  override def sink[A](
      streamName: String,
      settings: WriterSettings[A]
  ): Task[ZSink[Any, Throwable, A, Nothing, Unit]] = {
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

  override def stream[A](
      readerGroupName: String,
      settings: ReaderSettings[A]
  ): Task[ZStream[Any, Throwable, A]] = {

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
  def apply(eventStreamClientFactory: EventStreamClientFactory) =
    new PravegaStream(
      eventStreamClientFactory
    )
  def layer(
      scope: String,
      clientConfig: ClientConfig
  ): ZLayer[Any, Throwable, PravegaStreamService] = ZIO
    .attempt(EventStreamClientFactory.withScope(scope, clientConfig))
    .map(eventStreamClientFactory => Pravega(eventStreamClientFactory))
    .toManagedWith(pravega => UIO(pravega.eventStreamClientFactory.close()))
    .toLayer
}
