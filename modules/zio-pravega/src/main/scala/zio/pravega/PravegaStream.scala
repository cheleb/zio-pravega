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
 *
 * This API is a wrapper around the Pravega Java API.
 */
@Accessible
trait PravegaStream {

  /**
   * Writes atomicaly events to a stream.
   */
  def write[A](streamName: String, settings: WriterSettings[A], a: List[A]): ZIO[Any, Throwable, Unit]

  /**
   * Writes events to a transactional stream, transaction is created but not
   * committed.
   *
   * The transaction:
   *
   *   - is aborted in case of failure
   *   - must be committed by the caller
   *
   * @return
   *   transaction id
   */
  def writeTx[A](
    streamName: String,
    settings: WriterSettings[A],
    as: List[A]
  ): ZIO[Any, Throwable, UUID]

  /**
   * Sink that writes to a stream.
   */
  def sink[A](streamName: String, settings: WriterSettings[A]): Sink[Throwable, A, Nothing, Unit]

  /**
   * Sink that writes to a transactional stream.
   *
   * Transaction is created by the writer, and committed or aborted by the
   * writer regarding of the status of the stream scope.
   */
  def transactionalSink[A](streamName: String, settings: WriterSettings[A]): Sink[Throwable, A, Nothing, Unit]

  /**
   * Sink that writes to a transactional stream, transaction stays open after
   * the sink is closed.
   *
   * The transaction id is generated by the writer, once the transaction is
   * effectively created, and materialized when the sink is closed.
   *
   * The transaction is not committed or aborted by the writer, and can be used
   * by other writers, localy or remotely.
   *
   * @return
   *   The transaction id.
   */
  def newSharedTransactionalSink[A](
    streamName: String,
    settings: WriterSettings[A]
  ): Sink[Throwable, A, Nothing, UUID]

  /**
   * Sink that writes to a transactional stream, transaction stays open after
   * the sink is closed.
   *
   * The transaction id is provided by the caller.
   *
   * The transaction may be committed by the writer.
   */
  def sharedTransactionalSink[A](
    streamName: String,
    txUUID: UUID,
    settings: WriterSettings[A],
    commitOnExit: Boolean
  ): Sink[Throwable, A, Nothing, Unit]

  /**
   * Creates a ZPipeline that writes to a stream.
   */
  def writeFlow[A](streamName: String, settings: WriterSettings[A]): ZPipeline[Any, Throwable, A, A]

  /**
   * Stream (source) of elements.
   */
  def stream[A](readerGroupName: String, settings: ReaderSettings[A]): Stream[Throwable, A]

  /**
   * Stream (source) of events of elements.
   */
  def eventStream[A](readerGroupName: String, settings: ReaderSettings[A]): Stream[Throwable, EventRead[A]]
}

/**
 * Implementation of the Pravega Stream API.
 *
 * @param eventStreamClientFactory
 */
private class PravegaStreamImpl(eventStreamClientFactory: EventStreamClientFactory) extends PravegaStream {

  /**
   * Creates a Pravega EventWriter.
   *
   * This method is blocking and acquire a resource, and release it when the ZIO
   * is done.
   */
  private def createEventWriter[A](streamName: String, settings: WriterSettings[A]) = ZIO
    .attemptBlocking(
      eventStreamClientFactory.createEventWriter(streamName, settings.serializer, settings.eventWriterConfig)
    )
    .withFinalizerAuto

  /**
   * Creates a Pravega EventStreamReader.
   *
   * This method is blocking and acquire a resource, and release it when the ZIO
   * is done.
   */
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

  /**
   * Writes atomicaly events to a stream.
   */
  def write[A](streamName: String, settings: WriterSettings[A], as: List[A]): ZIO[Any, Throwable, Unit] =
    as match {
      case Nil => ZIO.unit
      case a :: Nil =>
        ZStream(a)
          .run(sink(streamName, settings))
      case _ =>
        ZStream(as*)
          .run(transactionalSink(streamName, settings))
    }

  def writeTx[A](
    streamName: String,
    settings: WriterSettings[A],
    as: List[A]
  ): ZIO[Any, Throwable, UUID] =
    ZStream
      .fromIterable(as)
      .run(newSharedTransactionalSink(streamName, settings))

  /**
   * Sink that writes to a stream.
   *
   * This sink is not transactional, and does not guarantee that the events are
   * written atomically.
   *
   * If you need to write atomically, use [[transactionalSink]] or
   * [[sharedTransactionalSink]].
   */
  def sink[A](streamName: String, settings: WriterSettings[A]): Sink[Throwable, A, Nothing, Unit] = ZSink.unwrapScoped(
    for {
      writer     <- createEventWriter(streamName, settings)
      eventWriter = EventWriter.writeEventTask(writer, settings)
    } yield ZSink.foreach(eventWriter)
  )

  /**
   * Creates a ZPipeline that writes to a stream.
   *
   * This pipeline is not transactional, and does not guarantee that the events
   * are written atomically.
   */
  def writeFlow[A](streamName: String, settings: WriterSettings[A]): ZPipeline[Any, Throwable, A, A] =
    ZPipeline.unwrapScoped {
      for (
        writer <- createEventWriter(streamName, settings); eventWriter = EventWriter.writeEventTask(writer, settings)
      ) yield ZPipeline.tap(eventWriter)
    }
    /**
     * Creates a Pravega TransactionalEventStreamWriter.
     *
     * This method is blocking and acquire a resource, and release it when the
     * ZIO is done. ZIO
     */
  private def createTxEventWriter[A](
    streamName: String,
    settings: WriterSettings[A]
  ): ZIO[Scope, Throwable, TransactionalEventStreamWriter[A]] = ZIO
    .attemptBlocking(
      eventStreamClientFactory
        .createTransactionalEventWriter(streamName, settings.serializer, settings.eventWriterConfig)
    )
    .withFinalizerAuto

  /**
   * Creates a Pravega TransactionalEventStreamWriter.
   *
   * This method is blocking and acquire a resource, but release it **only**
   * when the ZIO exits with a failure.
   */
  private def beginScopedUnclosingTransaction[A](
    writer: TransactionalEventStreamWriter[A]
  ): RIO[Scope, Transaction[A]] =
    ZIO.acquireReleaseExit(ZIO.attemptBlocking(writer.beginTxn)) {
      case (tx, Failure(e)) =>
        ZIO.logCause(e) *> ZIO.attemptBlocking(tx.abort()).orDie
      case (tx, Success(_)) =>
        ZIO.logDebug(s"Wrote to tx [${tx.getTxnId}]")
    }

  /**
   * Creates a Pravega TransactionalEventStreamWriter.
   *
   * This method is blocking and acquire a resource, and release it when the ZIO
   * is done.
   */
  private def beginScopedTransaction[A](writer: TransactionalEventStreamWriter[A]): RIO[Scope, Transaction[A]] =
    ZIO.acquireReleaseExit(ZIO.attemptBlocking(writer.beginTxn)) {
      case (tx, Failure(e)) =>
        ZIO.logCause(e) *> ZIO.attemptBlocking(tx.abort()).orDie
      case (tx, Success(_)) =>
        ZIO.attemptBlocking(tx.commit()).orDie
    }

  /**
   * Sink that writes to a transactional stream.
   *
   * Transaction is created by the writer, and committed or aborted regarding of
   * the exit status of the stream scope.
   */
  def transactionalSink[A](streamName: String, settings: WriterSettings[A]): Sink[Throwable, A, Nothing, Unit] =
    ZSink.unwrapScoped(
      for {
        writer        <- createTxEventWriter(streamName, settings)
        tx            <- beginScopedTransaction(writer)
        writeEventTask = EventWriter.writeEventTask(tx, settings)
      } yield ZSink.foreach(writeEventTask)
    )

  /**
   * Sink that writes to a transactional stream.
   *
   *   - The transaction id is generated by the writer, once the transaction is
   *     created.
   *   - The transaction is not committed **but aborted** in case of failure.
   */
  override def newSharedTransactionalSink[A](
    streamName: String,
    settings: WriterSettings[A]
  ): Sink[Throwable, A, Nothing, UUID] =
    ZSink.unwrapScoped(
      for {
        writer        <- createTxEventWriter(streamName, settings)
        tx            <- beginScopedUnclosingTransaction(writer)
        txUUID         = tx.getTxnId
        writeEventTask = EventWriter.writeEventTask(tx, settings)
      } yield ZSink.foldLeftZIO(txUUID)((tx, e) => writeEventTask(e) *> ZIO.succeed(txUUID))
    )

  /**
   * Sink that writes to an already existing transactional stream.
   *
   *   - The transaction id is provided by the caller.
   *   - The transaction may be committed by the writer, depending on the
   *     commitOnExit parameter.
   *   - The transaction is aborted in case of failure.
   */
  override def sharedTransactionalSink[A](
    streamName: String,
    txUUID: UUID,
    settings: WriterSettings[A],
    commitOnExit: Boolean
  ): Sink[Throwable, A, Nothing, Unit] = ZSink.unwrapScoped(
    for {
      writer <- createTxEventWriter(streamName, settings)

      txIO = ZIO.attemptBlocking(writer.getTxn(txUUID))
      tx <- if (commitOnExit)
              txIO.withFinalizerExit {
                case (tx, Failure(e)) =>
                  ZIO.logCause(e) *> ZIO.attemptBlocking(tx.abort()).orDie
                case (tx, Success(_)) =>
                  ZIO.logDebug(s"Commiting tx [$txUUID]") *> ZIO.attemptBlocking(tx.commit()).orDie
              }
            else txIO
      _ <- ZIO.unless(tx.checkStatus() == Transaction.Status.OPEN)(
             ZIO.dieMessage(s"Transaction $txUUID is not open")
           )

      writeEventTask = EventWriter.writeEventTask(tx, settings)
    } yield ZSink.foreach(writeEventTask)
  )

  /**
   * Reads the next event from the reader.
   *
   * This method is blocking.
   */
  private def readNextEvent[A](
    reader: EventStreamReader[A],
    timeout: Long
  ): Task[Chunk[A]] = ZIO.attemptBlocking(reader.readNextEvent(timeout) match {
    case eventRead if eventRead.isCheckpoint =>
      Chunk.empty
    case eventRead =>
      val event = eventRead.getEvent()
      if (event == null) Chunk.empty else Chunk.single(event)
  })

  /**
   * Stream (source) of elements.
   */
  def stream[A](readerGroupName: String, settings: ReaderSettings[A]): Stream[Throwable, A] = ZStream.unwrapScoped(
    for {
      reader  <- createEventStreamReader(readerGroupName, settings)
      readTask = readNextEvent(reader, settings.timeout)
    } yield ZStream.repeatZIOChunk(readTask)
  )

  /**
   * Stream (source) of events of elements.
   */
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

/**
 * Pravega Stream API.
 *
 * This API is a wrapper around the Pravega Java API.
 */
object PravegaStream {

  /**
   * Writes atomicaly to a stream. See [[zio.pravega.PravegaStream.write]].
   */
  def write[A](streamName: String, settings: WriterSettings[A], a: A*): ZIO[PravegaStream, Throwable, Unit] =
    ZIO.serviceWithZIO[PravegaStream](_.write(streamName, settings, a.toList))

  /**
   * Writes to a stream tx stream. See [[zio.pravega.PravegaStream.writeTx]].
   */
  def writeTx[A](streamName: String, settings: WriterSettings[A], a: A*): ZIO[PravegaStream, Throwable, UUID] =
    ZIO.serviceWithZIO[PravegaStream](_.writeTx(streamName, settings, a.toList))

  /**
   * Sink that writes to a stream. See [[zio.pravega.PravegaStream.sink]].
   */
  def sink[A](streamName: String, settings: WriterSettings[A]): ZSink[PravegaStream, Throwable, A, Nothing, Unit] =
    ZSink.serviceWithSink[PravegaStream](_.sink(streamName, settings))

  /**
   * Sink that writes to a transactional stream.
   */
  def transactionalSink[A](
    streamName: String,
    settings: WriterSettings[A]
  ): ZSink[PravegaStream, Throwable, A, Nothing, Unit] =
    ZSink.serviceWithSink[PravegaStream](_.transactionalSink(streamName, settings))

  /**
   * Sink that writes to a transactional stream.
   *   - The transaction id is generated by the writer, once the transaction is
   *     created.
   *   - The transaction is not committed
   *   - The transaction is aborted by the writer in case of failure.
   */
  def newSharedTransactionalSink[A](
    streamName: String,
    settings: WriterSettings[A]
  ): ZSink[PravegaStream, Throwable, A, Nothing, UUID] =
    ZSink.serviceWithSink[PravegaStream](_.newSharedTransactionalSink(streamName, settings))

  /**
   * Sink that writes to a transactional stream.
   *
   *   - The transaction id is provided by the caller.
   *   - The transaction may be committed by the writer.
   *   - The transaction is aborted by the writer in case of failure.
   */
  def sharedTransactionalSink[A](
    streamName: String,
    txUUID: UUID,
    settings: WriterSettings[A],
    commitOnExit: Boolean
  ): ZSink[PravegaStream, Throwable, A, Nothing, Unit] =
    ZSink.serviceWithSink[PravegaStream](_.sharedTransactionalSink(streamName, txUUID, settings, commitOnExit))
  def stream[A](readerGroupName: String, settings: ReaderSettings[A]): ZStream[PravegaStream, Throwable, A] =
    ZStream.serviceWithStream[PravegaStream](_.stream(readerGroupName, settings))

  /**
   * Stream of events. See [[zio.pravega.PravegaStream.eventStream]].
   */
  def eventStream[A](
    readerGroupName: String,
    settings: ReaderSettings[A]
  ): ZStream[PravegaStream, Throwable, EventRead[A]] =
    ZStream.serviceWithStream[PravegaStream](_.eventStream(readerGroupName, settings))

  /**
   * Creates a ZPipeline that writes to a stream.
   */
  def writeFlow[A](streamName: String, settings: WriterSettings[A]): ZPipeline[PravegaStream, Throwable, A, A] =
    ZPipeline.serviceWithPipeline[PravegaStream](_.writeFlow(streamName, settings))

  /**
   * Creates a Pravega stream Service from a scope.
   */
  private def streamService(scope: String, clientConfig: ClientConfig): ZIO[Scope, Throwable, PravegaStream] = ZIO
    .attemptBlocking(EventStreamClientFactory.withScope(scope, clientConfig))
    .withFinalizerAuto
    .map(new PravegaStreamImpl(_))

  /**
   * Creates a Pravega stream Service from a scope.
   *
   * Requires a ClientConfig to be provided in the environment.
   */
  def fromScope(scope: String): ZLayer[Scope & ClientConfig, Throwable, PravegaStream] =
    ZLayer.fromZIO(ZIO.serviceWithZIO[ClientConfig](streamService(scope, _)))
  def fromScope(scope: String, clientConfig: ClientConfig): ZLayer[Scope, Throwable, PravegaStream] = ZLayer(
    streamService(scope, clientConfig)
  )

}
