package zio

import io.pravega.client.ClientConfig
import io.pravega.client.EventStreamClientFactory

import io.pravega.client.stream.EventStreamReader

import java.util.UUID
import zio.stream.ZStream

import zio.stream.Sink
import zio.stream.ZSink
import io.pravega.client.stream.ReaderGroupConfig
import io.pravega.client

import zio.pravega._

object Pravega {

  trait Service extends AutoCloseable {
    def pravegaSink[A](streamName: String, settings: WriterSettings[A]): Sink[Any, A, A, Unit]
    def readerGroup[A](
      groupName: String,
      readerSettings: ReaderSettings[A],
      streamNames: String*
    ): ZIO[Any, Throwable, Unit]
    def pravegaStream[A](
      readerGroupName: String,
      settings: ReaderSettings[A]
    ): ZStream[Any, Throwable, A]
  }

  object Service {

    def live(scope: String, eventStreamClientFactory: EventStreamClientFactory) =
      new Service {

        def readerGroup[A](
          readerGroupName: String,
          readerSettings: ReaderSettings[A],
          streamNames: String*
        ): ZIO[Any, Throwable, Unit] = {
          def config() = {
            val builder = ReaderGroupConfig.builder()
            streamNames.foreach(name => builder.stream(client.stream.Stream.of(scope, name)))
            builder.build()
          }

          PravegaAdmin.readerGroupManager(scope, readerSettings.clientConfig).use { manager =>
            ZIO.attemptBlocking {
              manager.createReaderGroup(
                readerGroupName,
                config()
              )
            }
          }

        }

        override def pravegaStream[A](
          readerGroupName: String,
          settings: ReaderSettings[A]
        ): ZStream[Any, Throwable, A] = {
          case class QueuedReader(queue: Queue[A], reader: EventStreamReader[A])
          def next(qr: QueuedReader): RIO[Any, Unit] =
            for {
              m <- ZIO.attemptBlocking(qr.reader.readNextEvent(settings.timeout).getEvent())
              _ <- ZIO.unless(m == null)(qr.queue.offer(m))
              _ <- next(qr)
            } yield ()

          val reader = for {
            queue <- ZQueue.bounded[A](10)
            reader <- ZIO.attemptBlocking(
                       eventStreamClientFactory
                         .createReader(
                           settings.readerId.getOrElse(UUID.randomUUID().toString),
                           readerGroupName,
                           settings.serializer,
                           settings.readerConfig
                         )
                     )
            qr = QueuedReader(queue, reader)
            _  <- next(qr).fork

          } yield qr
          ZStream
            .acquireReleaseWith(reader)(qr => ZIO.attemptBlocking(qr.reader.close()).ignore)
            .flatMap(qr => ZStream.fromQueueWithShutdown(qr.queue))
        }

        override def pravegaSink[A](
          streamName: String,
          settings: WriterSettings[A]
        ): Sink[Any, A, A, Unit] = {
          val writer = ZIO
            .attemptBlocking(
              eventStreamClientFactory.createEventWriter(streamName, settings.serializer, settings.eventWriterConfig)
            )
            .toManagedWith(w => ZIO.attemptBlocking(w.close()).ignore)
          ZSink
            .managed(writer)(writer => ZSink.foreach((a: A) => ZIO.fromCompletableFuture(writer.writeEvent(a))))

        }

        override def close(): Unit =
          eventStreamClientFactory.close()
      }
  }

  def live(scope: String, clientConfig: ClientConfig): ZLayer[Any, Throwable, Has[Service]] =
    ZIO
      .attempt(EventStreamClientFactory.withScope(scope, clientConfig))
      .map(eventStreamClientFactory => Pravega.Service.live(scope, eventStreamClientFactory))
      .toManagedAuto
      .toLayer

  def pravegaSink[A](
    streamName: String,
    writterSettings: WriterSettings[A]
  ): ZIO[Has[Service], Throwable, Sink[Any, A, A, Unit]] =
    ZIO.access(p => p.get.pravegaSink[A](streamName, writterSettings))

  def pravegaStream[A](
    readerGroup: String,
    readerSettings: ReaderSettings[A]
  ): ZIO[Has[Service], Throwable, ZStream[Has[Service], Throwable, A]] =
    ZIO.access[Has[Service]](p => p.get.pravegaStream(readerGroup, readerSettings))

  def readerGroup[A](
    groupName: String,
    readerSettings: ReaderSettings[A],
    streamNames: String*
  ): ZIO[Has[Service], Throwable, Unit] =
    for {
      pravega <- ZIO.access[Has[Service]](_.get)
      _       <- pravega.readerGroup(groupName, readerSettings, streamNames: _*)
    } yield ()

}
