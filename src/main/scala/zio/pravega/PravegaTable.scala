package zio.pravega

import zio._
import zio.stream._
import io.pravega.client.KeyValueTableFactory
import io.pravega.client.tables.KeyValueTableClientConfiguration

import scala.jdk.CollectionConverters._
import io.pravega.client.ClientConfig
import io.pravega.client.tables.KeyValueTable
import io.pravega.client.tables.Version
import io.pravega.client.tables.{TableEntry => JTableEntry}
import io.pravega.client.tables.IteratorItem
import io.pravega.common.util.AsyncIterator
import java.util.concurrent.Executors

import zio.pravega.table.KVPWriter

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

  private def table(
    tableName: String,
    kvtClientConfig: KeyValueTableClientConfiguration
  ): ZIO[Scope, Throwable, KeyValueTable] = ZIO
    .attemptBlocking(keyValueTableFactory.forKeyValueTable(tableName, kvtClientConfig))
    .withFinalizerAuto

  private def upsert[K, V](
    k: K,
    v: V,
    table: KeyValueTable,
    kvpWriter: KVPWriter[K, V],
    combine: (V, V) => V
  ): ZIO[Any, Throwable, (Version, V)] = ZIO
    .fromCompletableFuture(table.get(kvpWriter.tableKey(k)))
    .map {
      case null     => kvpWriter.insert(k, v)
      case previous => kvpWriter.put(k, v, previous, combine)
    }
    .flatMap { case (mod, newValue) =>
      ZIO
        .fromCompletableFuture(table.update(mod))
        .map(version => (version, newValue))
        .tapError(o => ZIO.logDebug(o.toString))
    }
    .retry(Schedule.forever)

  override def sink[K, V](
    tableName: String,
    settings: TableWriterSettings[K, V],
    combine: (V, V) => V
  ): ZSink[Any, Throwable, (K, V), Nothing, Unit] = ZSink
    .unwrapScoped(table(tableName, settings.keyValueTableClientConfiguration).map { table =>
      val kvpWriter = new KVPWriter[K, V](settings)
      ZSink.foreach { case (k, v) => upsert(k, v, table, kvpWriter, combine) }
    })

  private def iterator(table: KeyValueTable, maxEntries: Int): AsyncIterator[IteratorItem[JTableEntry]] = table
    .iterator()
    .maxIterationSize(maxEntries)
    .all()
    .entries()
  override def source[K, V](
    tableName: String,
    settings: TableReaderSettings[K, V]
  ): ZStream[Any, Throwable, TableEntry[V]] = ZStream.unwrapScoped(for {
    table    <- table(tableName, settings.keyValueTableClientConfiguration)
    executor <- ZIO.succeed(Executors.newSingleThreadExecutor()).withFinalizerAuto
    it        = iterator(table, settings.maxEntriesAtOnce).asSequential(executor)
  } yield ZStream.repeatZIOChunkOption {
    ZIO.fromCompletableFuture(it.getNext()).asSomeError.flatMap {
      case null => ZIO.fail(None)
      case el =>
        val res = el.getItems().asScala.map { tableEntry =>
          TableEntry(
            tableEntry.getKey(),
            tableEntry.getVersion(),
            settings.valueSerializer.deserialize(tableEntry.getValue())
          )
        }
        ZIO.succeed(Chunk.fromArray(res.toArray))
    }
  })

  def writerFlow[K, V](
    tableName: String,
    settings: TableWriterSettings[K, V],
    combine: (V, V) => V
  ): ZPipeline[Any, Throwable, (K, V), (K, V)] = ZPipeline
    .unwrapScoped(table(tableName, settings.keyValueTableClientConfiguration).map { table =>
      ZPipeline.mapZIO { case (k, v) =>
        val kvpWriter = new KVPWriter[K, V](settings)
        upsert(k, v, table, kvpWriter, combine).map { case (_, newValue) => (k, newValue) }
      }
    })
  def readerFlow[K, V](
    tableName: String,
    settings: TableReaderSettings[K, V]
  ): ZPipeline[Any, Throwable, K, Option[TableEntry[V]]] = ZPipeline
    .unwrapScoped(table(tableName, settings.keyValueTableClientConfiguration).map { table =>
      ZPipeline.mapZIO { in =>
        ZIO.fromCompletableFuture(table.get(settings.tableKey(in))).map {
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
      }
    })
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

  /**
   * Create a Pravega Table API.
   *
   * @param scope
   * @param clientConfig
   */
  def fromScope(scope: String, clientConfig: ClientConfig): ZLayer[Scope, Throwable, PravegaTable] = ZLayer
    .fromZIO(service(scope, clientConfig))
}
