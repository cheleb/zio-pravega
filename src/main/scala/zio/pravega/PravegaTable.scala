package zio.pravega

import zio._
import zio.stream._
import io.pravega.client.KeyValueTableFactory
import io.pravega.client.tables.KeyValueTableClientConfiguration
import io.pravega.client.tables.Put

import scala.jdk.CollectionConverters._
import io.pravega.client.ClientConfig
import io.pravega.client.tables.KeyValueTable
import io.pravega.client.tables
import io.pravega.client.tables.IteratorItem
import io.pravega.common.util.AsyncIterator
import java.util.concurrent.Executors

trait PravegaTableService {
  def sink[K, V](
      tableName: String,
      settings: TableWriterSettings[K, V]
  ): RIO[Scope, ZSink[Any, Throwable, (K, V), Nothing, Unit]]

  def flow[K, V](
      tableName: String,
      settings: TableWriterSettings[K, V]
  ): RIO[Scope, ZPipeline[Any, Throwable, (K, V), TableEntry[V]]]

  def flow[K, V](
      tableName: String,
      settings: TableReaderSettings[K, V]
  ): RIO[Scope, ZPipeline[Any, Throwable, K, Option[TableEntry[V]]]]

  def source[K, V](
      tableName: String,
      settings: TableReaderSettings[K, V]
  ): RIO[Scope, ZStream[Any, Throwable, TableEntry[V]]]
}

object PravegaTableService {
  def sink[K, V](
      tableName: String,
      settings: TableWriterSettings[K, V]
  ): RIO[PravegaTableService & Scope, ZSink[
    Any,
    Throwable,
    (K, V),
    Nothing,
    Unit
  ]] =
    ZIO.serviceWithZIO[PravegaTableService](
      _.sink(tableName, settings)
    )

  def flow[K, V](
      tableName: String,
      settings: TableWriterSettings[K, V]
  ): RIO[PravegaTableService & Scope, ZPipeline[
    Any,
    Throwable,
    (K, V),
    TableEntry[V]
  ]] = ZIO.serviceWithZIO[PravegaTableService](
    _.flow(tableName, settings)
  )

  def flow[K, V](
      tableName: String,
      settings: TableReaderSettings[K, V]
  ): RIO[PravegaTableService & Scope, ZPipeline[Any, Throwable, K, Option[
    TableEntry[V]
  ]]] = ZIO.serviceWithZIO[PravegaTableService](
    _.flow(tableName, settings)
  )

  def source[K, V](
      tableName: String,
      settings: TableReaderSettings[K, V]
  ): RIO[PravegaTableService & Scope, ZStream[Any, Throwable, TableEntry[V]]] =
    ZIO
      .serviceWithZIO[PravegaTableService](
        _.source(tableName, settings)
      )

}

final case class PravegaTableServiceLive(
    keyValueTableFactory: KeyValueTableFactory
) extends PravegaTableService {

  private def table(
      tableName: String,
      kvtClientConfig: KeyValueTableClientConfiguration
  ): ZIO[Scope, Throwable, KeyValueTable] = ZIO
    .attemptBlocking(
      keyValueTableFactory.forKeyValueTable(tableName, kvtClientConfig)
    )
    .withFinalizerAuto

  override def sink[K, V](
      tableName: String,
      settings: TableWriterSettings[K, V]
  ): RIO[Scope, ZSink[Any, Throwable, (K, V), Nothing, Unit]] =
    table(tableName, settings.keyValueTableClientConfiguration)
      .map { table =>
        ZSink.foreach { case (k, v) =>
          ZIO
            .fromCompletableFuture(table.get(settings.tableKey(k)))
            .map {
              case null =>
                new tables.Insert(
                  settings.tableKey(k),
                  settings.valueSerializer.serialize(v)
                )
              case el =>
                new tables.Put(
                  settings.tableKey(k),
                  settings.valueSerializer
                    .serialize(
                      settings.combine(
                        settings.valueSerializer.deserialize(el.getValue),
                        v
                      )
                    ),
                  el.getVersion
                )
            }
            .flatMap { mod =>
              ZIO
                .fromCompletableFuture(table.update(mod))
                .tapError(o => ZIO.debug(o.toString))
            }
            .retry(Schedule.forever)
        }
      }

  private def iterator(
      table: KeyValueTable,
      maxEntries: Int
  ): AsyncIterator[IteratorItem[tables.TableEntry]] = table
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

  def flow[K, V](
      tableName: String,
      settings: TableWriterSettings[K, V]
  ): RIO[Scope, ZPipeline[Any, Throwable, (K, V), TableEntry[V]]] =
    table(tableName, settings.keyValueTableClientConfiguration)
      .map { table =>
        ZPipeline.mapZIO { case (k, v) =>
          val put =
            new Put(settings.tableKey(k), settings.valueSerializer.serialize(v))
          ZIO
            .fromCompletableFuture(table.update(put))
            .map(version =>
              TableEntry(
                settings.tableKey(k),
                version,
                v
              )
            )
        }
      }
  def flow[K, V](
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

object PravegaTableLayer {

  private def service(
      scope: String
  ): ZIO[ClientConfig & Scope, Throwable, PravegaTableService] = {

    def acquire(clientConfig: ClientConfig) = ZIO
      .attemptBlocking(
        KeyValueTableFactory.withScope(scope, clientConfig)
      )

    def release(fac: KeyValueTableFactory) =
      ZIO.attemptBlocking(fac.close()).ignore

    for {
      clientConfig <- ZIO.service[ClientConfig]
      clientFactory <- ZIO
        .acquireRelease(acquire(clientConfig))(release)
    } yield PravegaTableServiceLive(clientFactory)
  }

  def fromScope(
      scope: String
  ): ZLayer[ClientConfig & Scope, Throwable, PravegaTableService] = ZLayer(
    service(
      scope
    )
  )

}
