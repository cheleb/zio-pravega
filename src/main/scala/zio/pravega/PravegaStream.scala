package zio.pravega

import io.pravega.client.ClientConfig
import io.pravega.client.EventStreamClientFactory

import zio._

import zio.stream._

import java.util.UUID
import io.pravega.client.stream.EventStreamWriter

trait PravegaStreamService {
  def sink[A](
      streamName: String,
      settings: WriterSettings[A]
  ): RIO[Scope, ZSink[Any, Throwable, A, Nothing, Unit]]
  def stream[A](
      readerGroupName: String,
      settings: ReaderSettings[A]
  ): Task[ZStream[Any, Throwable, A]]
}

object PravegaStream extends Accessible[PravegaStreamService]

case class PravegaStream(eventStreamClientFactory: EventStreamClientFactory)
    extends PravegaStreamService {

  override def sink[A](
      streamName: String,
      settings: WriterSettings[A]
  ): RIO[Scope, ZSink[Any, Throwable, A, Nothing, Unit]] = {
    val acquireWriter = ZIO
      .attemptBlocking(
        eventStreamClientFactory.createEventWriter(
          streamName,
          settings.serializer,
          settings.eventWriterConfig
        )
      )
    def release(writer: EventStreamWriter[A]) =
      ZIO.attemptBlocking(writer.close()).ignore

    val si = ZIO
      .acquireRelease(acquireWriter)(release)
      .map(writer =>
        ZSink.foreach((a: A) => ZIO.fromCompletableFuture(writer.writeEvent(a)))
      )

    RIO.attempt(
      ZSink.unwrapScoped(si)
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

    Task.attempt(
      ZStream
        .acquireReleaseWith(reader)(reader =>
          ZIO.attemptBlocking(reader.close()).ignore
        )
        .flatMap(reader =>
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

  }

}

object PravegaStreamLayer {

  def service(
      scope: String
  ): ZIO[ClientConfig & Scope, Throwable, PravegaStreamService] = {

    def acquire(clientConfig: ClientConfig) = ZIO
      .attemptBlocking(
        EventStreamClientFactory.withScope(scope, clientConfig)
      )

    def release(fac: EventStreamClientFactory) =
      URIO.attemptBlocking(fac.close()).ignore

    for {
      clientConfig <- ZIO.service[ClientConfig]
      clientFactory <- ZIO
        .acquireRelease(acquire(clientConfig))(release)
    } yield PravegaStream(clientFactory)
  }

  def fromScope(
      scope: String
  ): ZLayer[ClientConfig & Scope, Throwable, PravegaStreamService] = ZLayer(
    service(
      scope
    )
  )

}
