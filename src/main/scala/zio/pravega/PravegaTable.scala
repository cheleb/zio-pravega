package zio.pravega

import zio._
import zio.stream._
import io.pravega.client.KeyValueTableFactory
import io.pravega.client.tables.KeyValueTableClientConfiguration

import scala.jdk.CollectionConverters._
import io.pravega.client.ClientConfig
import io.pravega.client.tables.KeyValueTable
import io.pravega.client.tables.Version
import io.pravega.client.tables.Insert
import io.pravega.client.tables.Put
import io.pravega.client.tables.{TableEntry => JTableEntry}
import io.pravega.client.tables.IteratorItem
import io.pravega.common.util.AsyncIterator
import java.util.concurrent.Executors

/** Pravega Table API.
  */
trait PravegaTable {

  /** Create a sink
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
  ): RIO[Scope, ZSink[Any, Throwable, (K, V), Nothing, Unit]]

  /** Create a writer flow
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
  ): RIO[Scope, ZPipeline[Any, Throwable, (K, V), TableEntry[V]]]

  /** Create a reader flow
    *
    * @param tableName
    * @param settings
    */
  def readerFlow[K, V](
      tableName: String,
      settings: TableReaderSettings[K, V]
  ): RIO[Scope, ZPipeline[Any, Throwable, K, Option[TableEntry[V]]]]

  /** Create a reader source
    *
    * @param tableName
    * @param settings
    */
  def source[K, V](
      tableName: String,
      settings: TableReaderSettings[K, V]
  ): RIO[Scope, ZStream[Any, Throwable, TableEntry[V]]]
}

/** Pravega Table API.
  */
object PravegaTable {
  def sink[K, V](
      tableName: String,
      settings: TableWriterSettings[K, V],
      combine: (V, V) => V
  ): RIO[PravegaTable & Scope, ZSink[
    Any,
    Throwable,
    (K, V),
    Nothing,
    Unit
  ]] =
    ZIO.serviceWithZIO[PravegaTable](
      _.sink(tableName, settings, combine)
    )

  def writerFlow[K, V](
      tableName: String,
      settings: TableWriterSettings[K, V],
      combine: (V, V) => V
  ): RIO[PravegaTable & Scope, ZPipeline[
    Any,
    Throwable,
    (K, V),
    TableEntry[V]
  ]] = ZIO.serviceWithZIO[PravegaTable](
    _.writerFlow(tableName, settings, combine)
  )

  def readerFlow[K, V](
      tableName: String,
      settings: TableReaderSettings[K, V]
  ): RIO[PravegaTable & Scope, ZPipeline[Any, Throwable, K, Option[
    TableEntry[V]
  ]]] = ZIO.serviceWithZIO[PravegaTable](
    _.readerFlow(tableName, settings)
  )

  def source[K, V](
      tableName: String,
      settings: TableReaderSettings[K, V]
  ): RIO[PravegaTable & Scope, ZStream[Any, Throwable, TableEntry[V]]] =
    ZIO
      .serviceWithZIO[PravegaTable](
        _.source(tableName, settings)
      )

  private def service(
      scope: String,
      clientConfig: ClientConfig
  ): ZIO[Scope, Throwable, PravegaTable] =
    for {
      clientFactory <- ZIO
        .attemptBlocking(
          KeyValueTableFactory.withScope(scope, clientConfig)
        )
        .withFinalizerAuto
    } yield new PravegaTableImpl(clientFactory)

  /** Create a Pravega Table API.
    *
    * @param scope
    * @param clientConfig
    */
  def fromScope(
      scope: String,
      clientConfig: ClientConfig
  ): ZLayer[Scope, Throwable, PravegaTable] =
    ZLayer.fromZIO(
      service(
        scope,
        clientConfig
      )
    )
}

private final case class PravegaTableImpl(
    keyValueTableFactory: KeyValueTableFactory
) extends PravegaTable {

  private def table(
      tableName: String,
      kvtClientConfig: KeyValueTableClientConfiguration
  ): ZIO[Scope, Throwable, KeyValueTable] = ZIO
    .attemptBlocking(
      keyValueTableFactory.forKeyValueTable(tableName, kvtClientConfig)
    )
    .withFinalizerAuto

  private def upsert[K, V](
      k: K,
      v: V,
      table: KeyValueTable,
      settings: TableWriterSettings[K, V],
      combine: (V, V) => V
  ): ZIO[Any, Throwable, Version] =
    ZIO
      .fromCompletableFuture(table.get(settings.tableKey(k)))
      .map {
        case null =>
          new Insert(
            settings.tableKey(k),
            settings.valueSerializer.serialize(v)
          )
        case previous =>
          new Put(
            settings.tableKey(k),
            settings.valueSerializer
              .serialize(
                combine(
                  settings.valueSerializer.deserialize(previous.getValue),
                  v
                )
              ),
            previous.getVersion
          )
      }
      .flatMap { mod =>
        ZIO
          .fromCompletableFuture(table.update(mod))
          .tapError(o => ZIO.logDebug(o.toString))
      }
      .retry(Schedule.forever)

  override def sink[K, V](
      tableName: String,
      settings: TableWriterSettings[K, V],
      combine: (V, V) => V
  ): RIO[Scope, ZSink[Any, Throwable, (K, V), Nothing, Unit]] =
    table(tableName, settings.keyValueTableClientConfiguration)
      .map { table =>
        ZSink.foreach { case (k, v) =>
          upsert(k, v, table, settings, combine)
        }
      }

  private def iterator(
      table: KeyValueTable,
      maxEntries: Int
  ): AsyncIterator[IteratorItem[JTableEntry]] = table
    .iterator()
    .maxIterationSize(maxEntries)
    .all()
    .entries()
  override def source[K, V](
      tableName: String,
      settings: TableReaderSettings[K, V]
  ): RIO[Scope, ZStream[Any, Throwable, TableEntry[V]]] =
    for {
      table <- table(tableName, settings.keyValueTableClientConfiguration)
      it = iterator(table, settings.maxEntriesAtOnce).asSequential(
        Executors.newSingleThreadExecutor()
      )
    } yield ZStream
      .repeatZIOChunkOption {
        ZIO
          .fromCompletableFuture(it.getNext())
          .asSomeError
          .flatMap {
            case null =>
              ZIO.fail(None)
            case el =>
              val res = el.getItems().asScala.map { tableEntry =>
                TableEntry(
                  tableEntry.getKey(),
                  tableEntry.getVersion(),
                  settings.valueSerializer
                    .deserialize(tableEntry.getValue())
                )
              }
              ZIO
                .succeed(Chunk.fromArray(res.toArray))
          }
      }

  def writerFlow[K, V](
      tableName: String,
      settings: TableWriterSettings[K, V],
      combine: (V, V) => V
  ): RIO[Scope, ZPipeline[Any, Throwable, (K, V), TableEntry[V]]] =
    table(tableName, settings.keyValueTableClientConfiguration)
      .map { table =>
        ZPipeline.mapZIO { case (k, v) =>
          upsert(k, v, table, settings, combine)
            .map(version =>
              TableEntry(
                settings.tableKey(k),
                version,
                v
              )
            )
        }
      }
  def readerFlow[K, V](
      tableName: String,
      settings: TableReaderSettings[K, V]
  ): RIO[Scope, ZPipeline[Any, Throwable, K, Option[TableEntry[V]]]] =
    table(tableName, settings.keyValueTableClientConfiguration)
      .map { table =>
        ZPipeline.mapZIO { in =>
          ZIO
            .fromCompletableFuture(table.get(settings.tableKey(in)))
            .map {
              case null => None
              case tableEntry =>
                Some(
                  TableEntry(
                    tableEntry.getKey(),
                    tableEntry.getVersion(),
                    settings.valueSerializer
                      .deserialize(tableEntry.getValue())
                  )
                )
            }
        }
      }

}
