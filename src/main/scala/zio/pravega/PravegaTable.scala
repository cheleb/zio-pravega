package zio.pravega

import zio._
import zio.stream._
import io.pravega.client.KeyValueTableFactory

import scala.jdk.CollectionConverters._
import io.pravega.client.ClientConfig
import io.pravega.client.tables.KeyValueTable
import io.pravega.client.tables.Version
import io.pravega.client.tables.{TableEntry => JTableEntry}
import io.pravega.client.tables.IteratorItem
import io.pravega.common.util.AsyncIterator
import java.util.concurrent.Executors

import zio.pravega.table.PravegaKeyValueTable
import io.pravega.client.stream.Serializer

/**
 * Pravega Table API.
 */
trait PravegaTable {

  /**
   * Create a sink
   *
   * @param tableName
   * @param settings
   * @param combine
   *   old and new entries.
   */
  def sink[K, V](
    tableName: String,
    settings: TableWriterSettings[K, V],
    combine: (V, V) => V
  ): ZSink[Any, Throwable, (K, V), Nothing, Unit]

  /**
   * Create a writer flow
   *
   * @param tableName
   * @param settings
   * @param combine
   *   old and new entries.
   */
  def writerFlow[K, V](
    tableName: String,
    settings: TableWriterSettings[K, V],
    combine: (V, V) => V
  ): ZPipeline[Any, Throwable, (K, V), (K, V)]

  /**
   * Create a reader flow
   *
   * @param tableName
   * @param settings
   */
  def readerFlow[K, V](
    tableName: String,
    settings: TableReaderSettings[K, V]
  ): ZPipeline[Any, Throwable, K, Option[TableEntry[V]]]

  /**
   * Create a reader source
   *
   * @param tableName
   * @param settings
   */
  def source[K, V](tableName: String, settings: TableReaderSettings[K, V]): ZStream[Any, Throwable, TableEntry[V]]
}

private final case class PravegaTableImpl(keyValueTableFactory: KeyValueTableFactory) extends PravegaTable {

  private def connectTable[K, V](
    tableName: String,
    tableSetting: TableSettings[K, V]
  ): ZIO[Scope, Throwable, PravegaKeyValueTable[K, V]] = ZIO
    .attemptBlocking(keyValueTableFactory.forKeyValueTable(tableName, tableSetting.keyValueTableClientConfiguration))
    .withFinalizerAuto
    .map(PravegaKeyValueTable(_, tableSetting))

  private def upsert[K, V](
    k: K,
    v: V,
    table: PravegaKeyValueTable[K, V],
    combine: (V, V) => V
  ): ZIO[Any, Throwable, (Version, V)] =
    (for {
      updateMod <- table.updateTask(k, v, combine)
      result    <- table.pushUpdate(updateMod)
    } yield result).retry(Schedule.forever)

  override def sink[K, V](
    tableName: String,
    settings: TableWriterSettings[K, V],
    combine: (V, V) => V
  ): ZSink[Any, Throwable, (K, V), Nothing, Unit] = ZSink
    .unwrapScoped(
      connectTable(tableName, settings)
        .map(table => ZSink.foreach { case (k, v) => upsert(k, v, table, combine) })
    )

  private def iterator(table: KeyValueTable, maxEntries: Int): AsyncIterator[IteratorItem[JTableEntry]] = table
    .iterator()
    .maxIterationSize(maxEntries)
    .all()
    .entries()

  private def readNextEntry[V](
    it: AsyncIterator[IteratorItem[JTableEntry]],
    valueSerializer: Serializer[V]
  ): IO[Option[Throwable], Chunk[TableEntry[V]]] =
    ZIO.fromCompletableFuture(it.getNext()).asSomeError.flatMap {
      case null => ZIO.fail(None)
      case el =>
        val res = el.getItems().asScala.map { tableEntry =>
          TableEntry(
            tableEntry.getKey(),
            tableEntry.getVersion(),
            valueSerializer.deserialize(tableEntry.getValue())
          )
        }
        ZIO.succeed(Chunk.fromArray(res.toArray))
    }
  override def source[K, V](
    tableName: String,
    settings: TableReaderSettings[K, V]
  ): ZStream[Any, Throwable, TableEntry[V]] = ZStream.unwrapScoped(
    for {
      table      <- connectTable(tableName, settings)
      executor   <- ZIO.succeed(Executors.newSingleThreadExecutor()).withFinalizerAuto
      it          = iterator(table.table, settings.maxEntriesAtOnce).asSequential(executor)
      nextEntryIO = readNextEntry(it, settings.valueSerializer)
    } yield ZStream.repeatZIOChunkOption(nextEntryIO)
  )

  def writerFlow[K, V](
    tableName: String,
    settings: TableWriterSettings[K, V],
    combine: (V, V) => V
  ): ZPipeline[Any, Throwable, (K, V), (K, V)] = ZPipeline
    .unwrapScoped(
      connectTable(tableName, settings)
        .map(table =>
          ZPipeline.mapZIO { case (k, v) =>
            upsert(k, v, table, combine).map { case (_, newValue) => (k, newValue) }
          }
        )
    )

  private def readEntryIO[K, V](
    table: KeyValueTable,
    settings: TableReaderSettings[K, V]
  ): K => Task[Option[TableEntry[V]]] =
    (k: K) =>
      ZIO.fromCompletableFuture(table.get(settings.tableKey(k))).map {
        case null => None
        case tableEntry =>
          Some(
            TableEntry(
              tableEntry.getKey(),
              tableEntry.getVersion(),
              settings.valueSerializer.deserialize(tableEntry.getValue())
            )
          )
      }

  def readerFlow[K, V](
    tableName: String,
    settings: TableReaderSettings[K, V]
  ): ZPipeline[Any, Throwable, K, Option[TableEntry[V]]] =
    ZPipeline
      .unwrapScoped(
        for {
          table      <- connectTable(tableName, settings)
          entryIoForK = readEntryIO(table.table, settings)
        } yield ZPipeline.mapZIO(entryIoForK)
      )
}

/**
 * Pravega Table API.
 */
object PravegaTable {
  def sink[K, V](
    tableName: String,
    settings: TableWriterSettings[K, V],
    combine: (V, V) => V
  ): ZSink[PravegaTable, Throwable, (K, V), Nothing, Unit] = ZSink
    .serviceWithSink[PravegaTable](_.sink(tableName, settings, combine))

  /**
   * Create a writer flow _____ (K,V) --->|_____|--> (K, V)
   */
  def writerFlow[K, V](
    tableName: String,
    settings: TableWriterSettings[K, V],
    combine: (V, V) => V
  ): ZPipeline[PravegaTable, Throwable, (K, V), (K, V)] = ZPipeline
    .serviceWithPipeline[PravegaTable](_.writerFlow(tableName, settings, combine))

  def readerFlow[K, V](
    tableName: String,
    settings: TableReaderSettings[K, V]
  ): ZPipeline[PravegaTable, Throwable, K, Option[TableEntry[V]]] = ZPipeline
    .serviceWithPipeline[PravegaTable](_.readerFlow(tableName, settings))

  def source[K, V](
    tableName: String,
    settings: TableReaderSettings[K, V]
  ): ZStream[PravegaTable, Throwable, TableEntry[V]] = ZStream
    .serviceWithStream[PravegaTable](_.source(tableName, settings))

  private def service(scope: String, clientConfig: ClientConfig): ZIO[Scope, Throwable, PravegaTable] = for {
    clientFactory <- ZIO.attemptBlocking(KeyValueTableFactory.withScope(scope, clientConfig)).withFinalizerAuto
  } yield new PravegaTableImpl(clientFactory)

  def fromScope(scope: String): ZLayer[Scope & ClientConfig, Throwable, PravegaTable] =
    ZLayer.fromZIO(ZIO.serviceWithZIO[ClientConfig](service(scope, _)))
  def fromScope(scope: String, clientConfig: ClientConfig): ZLayer[Scope, Throwable, PravegaTable] =
    ZLayer.fromZIO(service(scope, clientConfig))
}
