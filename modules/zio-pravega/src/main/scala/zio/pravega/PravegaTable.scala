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
   * Insert a value in a Pravega table, may overwrite existing values.
   */
  def put[K, V](tableName: String, key: K, value: V, settings: TableWriterSettings[K, V]): Task[Unit]

  /**
   * Upsert a value in a Pravega table, may overwrite existing values.
   */
  def merge[K, V](
    tableName: String,
    key: K,
    value: V,
    combine: (V, V) => V,
    settings: TableWriterSettings[K, V]
  ): Task[V]

  /**
   * Get a value from a Pravega table.
   */
  def get[K, V](
    tableName: String,
    key: K,
    settings: TableReaderSettings[K, V]
  ): Task[Option[V]]

  /**
   * Connect to a Pravega table.
   *
   * The table is closed when the scope is closed.
   */
  def openTable[K, V](
    tableName: String,
    tableSetting: TableSettings[K, V]
  ): ZIO[Scope, Throwable, PravegaKeyValueTable[K, V]]

  /**
   * Create a sink to write to a KVP Pravega table.
   *
   * A @combine function is used to merge old and new entries.
   */
  def sink[K, V](
    tableName: String,
    settings: TableWriterSettings[K, V],
    combine: (V, V) => V
  ): ZSink[Any, Throwable, (K, V), Nothing, Unit]

  /**
   * Create a writer flow to a KVP Pravega table.
   *
   * A @combine function is used to merge old and new entries, and new values
   * are emitted.
   */
  def writerFlow[K, V](
    tableName: String,
    settings: TableWriterSettings[K, V],
    combine: (V, V) => V
  ): ZPipeline[Any, Throwable, (K, V), (K, V)]

  /**
   * Create a reader flow, which reads from a KVP Pravega table.
   *
   * The reader flow emits an optional value, which is None if the key is not
   * found.
   */
  def readerFlow[K, V](
    tableName: String,
    settings: TableReaderSettings[K, V]
  ): ZPipeline[Any, Throwable, K, Option[TableEntry[V]]]

  /**
   * Create a reader source, which reads from a KVP Pravega table.
   *
   * Read entries are emitted in chunks.
   */
  def source[K, V](tableName: String, settings: TableReaderSettings[K, V]): ZStream[Any, Throwable, TableEntry[V]]
}

/**
 * Pravega Table API.
 */
private final case class PravegaTableLive(keyValueTableFactory: KeyValueTableFactory) extends PravegaTable {

  /**
   * Upsert a value in a Pravega table, may overwrite existing values.
   *
   * In case of a conflict, the ZIO will fail and will be retried.
   */
  override def merge[K, V](
    tableName: String,
    key: K,
    value: V,
    combine: (V, V) => V,
    settings: TableWriterSettings[K, V]
  ): Task[V] =
    ZIO.scoped {
      openTable(tableName, settings).flatMap { table =>
        table
          .updateTask(key, value, combine)
          .flatMap(table.pushUpdate)
          .retry(Schedule.forever)
          .map(_._2)
      }
    }

  /**
   * Insert a value in a Pravega table, may overwrite existing values.
   */
  override def put[K, V](tableName: String, key: K, value: V, settings: TableWriterSettings[K, V]): Task[Unit] =
    ZIO.scoped {
      openTable(tableName, settings).flatMap { table =>
        table.overrideTask(key, value).flatMap(table.pushUpdate)
      }.unit
    }

  /**
   * Get a value from a Pravega table.
   */
  override def get[K, V](tableName: String, key: K, settings: TableReaderSettings[K, V]): Task[Option[V]] =
    ZIO.scoped {
      openTable(tableName, settings).flatMap { table =>
        table.get(key)
      }
    }

  /**
   * Connect to a Pravega table.
   */
  def openTable[K, V](
    tableName: String,
    tableSetting: TableSettings[K, V]
  ): ZIO[Scope, Throwable, PravegaKeyValueTable[K, V]] = ZIO
    .attemptBlocking(keyValueTableFactory.forKeyValueTable(tableName, tableSetting.keyValueTableClientConfiguration))
    .withFinalizerAuto
    .map(PravegaKeyValueTable(_, tableSetting))

  /**
   * Upsert a value in a Pravega table, will retry forever.
   */
  private def upsert[K, V](
    k: K,
    v: V,
    table: PravegaKeyValueTable[K, V],
    combine: (V, V) => V
  ): Task[(Version, V)] =
    (for {
      updateMod <- table.updateTask(k, v, combine)
      result    <- table.pushUpdate(updateMod)
    } yield result).retry(Schedule.forever)

  /**
   * Create a sink to write to a KVP Pravega table.
   */
  override def sink[K, V](
    tableName: String,
    settings: TableWriterSettings[K, V],
    combine: (V, V) => V
  ): ZSink[Any, Throwable, (K, V), Nothing, Unit] = ZSink
    .unwrapScoped(
      openTable(tableName, settings)
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
      case el   =>
        val res = el.getItems().asScala.map { tableEntry =>
          TableEntry(
            tableEntry.getKey(),
            tableEntry.getVersion(),
            valueSerializer.deserialize(tableEntry.getValue())
          )
        }
        ZIO.succeed(Chunk.fromArray(res.toArray))
    }

  /**
   * Create a reader source, which reads from a KVP Pravega table.
   *
   * Read entries are emitted in chunks.
   */
  override def source[K, V](
    tableName: String,
    settings: TableReaderSettings[K, V]
  ): ZStream[Any, Throwable, TableEntry[V]] = ZStream.unwrapScoped(
    for {
      table    <- openTable(tableName, settings)
      executor <-
        ZIO.succeed(Executors.newSingleThreadExecutor()).withFinalizer(e => ZIO.attemptBlocking(e.shutdown()).ignore)
      it          = iterator(table.table, settings.maxEntriesAtOnce).asSequential(executor)
      nextEntryIO = readNextEntry(it, settings.valueSerializer)
    } yield ZStream.repeatZIOChunkOption(nextEntryIO)
  )

  /**
   * Create a writer flow to a KVP Pravega table.
   *
   * A @combine function is used to merge old and new entries, and new values
   * are emitted.
   */
  def writerFlow[K, V](
    tableName: String,
    settings: TableWriterSettings[K, V],
    combine: (V, V) => V
  ): ZPipeline[Any, Throwable, (K, V), (K, V)] = ZPipeline
    .unwrapScoped(
      openTable(tableName, settings)
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
        case null       => None
        case tableEntry =>
          Some(
            TableEntry(
              tableEntry.getKey(),
              tableEntry.getVersion(),
              settings.valueSerializer.deserialize(tableEntry.getValue())
            )
          )
      }

  /**
   * Create a reader flow, which reads from a KVP Pravega table.
   *
   * The reader flow emits an optional value, which is None if the key is not
   * found.
   */
  def readerFlow[K, V](
    tableName: String,
    settings: TableReaderSettings[K, V]
  ): ZPipeline[Any, Throwable, K, Option[TableEntry[V]]] =
    ZPipeline
      .unwrapScoped(
        for {
          table      <- openTable(tableName, settings)
          entryIoForK = readEntryIO(table.table, settings)
        } yield ZPipeline.mapZIO(entryIoForK)
      )
}

/**
 * Pravega Table API companion object with accessible methods.
 */
object PravegaTable {

  /**
   * Insert a value in a Pravega table, may overwrite existing values.
   *
   * In case of a conflict, the ZIO will fail and will be retried.
   */
  def put[K, V](
    tableName: String,
    key: K,
    value: V,
    settings: TableWriterSettings[K, V]
  ): ZIO[PravegaTable, Throwable, Unit] =
    ZIO.serviceWithZIO[PravegaTable](_.put(tableName, key, value, settings))

  def merge[K, V](
    tableName: String,
    key: K,
    value: V,
    combine: (V, V) => V,
    settings: TableWriterSettings[K, V]
  ): ZIO[PravegaTable, Throwable, V] =
    ZIO.serviceWithZIO[PravegaTable](_.merge(tableName, key, value, combine, settings))

  /**
   * Get a value from a Pravega table.
   */
  def get[K, V](
    tableName: String,
    key: K,
    settings: TableReaderSettings[K, V]
  ): ZIO[PravegaTable, Throwable, Option[V]] =
    ZIO.serviceWithZIO[PravegaTable](_.get(tableName, key, settings))

  def openTable[K, V](
    tableName: String,
    tableSetting: TableSettings[K, V]
  ): ZIO[Scope & PravegaTable, Throwable, PravegaKeyValueTable[K, V]] =
    ZIO.serviceWithZIO[PravegaTable](_.openTable(tableName, tableSetting))

  def sink[K, V](
    tableName: String,
    settings: TableWriterSettings[K, V],
    combine: (V, V) => V
  ): ZSink[PravegaTable, Throwable, (K, V), Nothing, Unit] = ZSink
    .serviceWithSink[PravegaTable](_.sink(tableName, settings, combine))

  /**
   * Create a writer flow to a KVP Pravega table.
   *
   * A @combine function is used to merge old and new entries, and new values
   * are emitted.
   */
  def writerFlow[K, V](
    tableName: String,
    settings: TableWriterSettings[K, V],
    combine: (V, V) => V
  ): ZPipeline[PravegaTable, Throwable, (K, V), (K, V)] = ZPipeline
    .serviceWithPipeline[PravegaTable](_.writerFlow(tableName, settings, combine))

  /**
   * Create a reader flow, which reads from a KVP Pravega table.
   *
   * The reader flow emits an optional value, which is None if the key is not
   * found.
   */
  def readerFlow[K, V](
    tableName: String,
    settings: TableReaderSettings[K, V]
  ): ZPipeline[PravegaTable, Throwable, K, Option[TableEntry[V]]] = ZPipeline
    .serviceWithPipeline[PravegaTable](_.readerFlow(tableName, settings))

  /**
   * Create a reader source, which reads from a KVP Pravega table.
   *
   * Read entries are emitted in chunks.
   */
  def source[K, V](
    tableName: String,
    settings: TableReaderSettings[K, V]
  ): ZStream[PravegaTable, Throwable, TableEntry[V]] = ZStream
    .serviceWithStream[PravegaTable](_.source(tableName, settings))

  private def service(scope: String, clientConfig: ClientConfig): ZIO[Scope, Throwable, PravegaTable] = for {
    clientFactory <- ZIO.attemptBlocking(KeyValueTableFactory.withScope(scope, clientConfig)).withFinalizerAuto
  } yield new PravegaTableLive(clientFactory)

  /**
   * Create a PravegaTable layer from a scope name and a client config found in
   * the environment.
   */
  def fromScope(scope: String): ZLayer[Scope & ClientConfig, Throwable, PravegaTable] =
    ZLayer.fromZIO(ZIO.serviceWithZIO[ClientConfig](service(scope, _)))

  /**
   * Create a PravegaTable layer from a scope name and a client config.
   */
  def fromScope(scope: String, clientConfig: ClientConfig): ZLayer[Scope, Throwable, PravegaTable] =
    ZLayer.fromZIO(service(scope, clientConfig))
}
