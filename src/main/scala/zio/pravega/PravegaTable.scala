package zio.pravega

import zio._
import zio.stream._
import io.pravega.client.KeyValueTableFactory
import io.pravega.client.tables.KeyValueTableClientConfiguration
import io.pravega.client.tables.Put

import zio.Task

import scala.jdk.CollectionConverters._
import io.pravega.client.ClientConfig
import io.pravega.client.tables.KeyValueTable

trait PravegaTableService {
  def sink[K, V](
      tableName: String,
      settings: TableWriterSettings[K, V],
      kvtClientConfig: KeyValueTableClientConfiguration
  ): RIO[Scope, ZSink[Any, Throwable, (K, V), Nothing, Unit]]

  def flow[K, V](
      tableName: String,
      settings: TableWriterSettings[K, V],
      kvtClientConfig: KeyValueTableClientConfiguration
  ): RIO[Scope, ZPipeline[Any, Throwable, (K, V), TableEntry[V]]]

  def flow[K, V](
      tableName: String,
      settings: TableReaderSettings[K, V],
      kvtClientConfig: KeyValueTableClientConfiguration
  ): RIO[Scope, ZPipeline[Any, Throwable, K, Option[TableEntry[V]]]]

  def source[K, V](
      readerGroupName: String,
      settings: TableReaderSettings[K, V],
      kvtClientConfig: KeyValueTableClientConfiguration
  ): RIO[Scope, ZStream[Any, Throwable, TableEntry[V]]]
}

object PravegaTable extends Accessible[PravegaTableService]

final case class PravegaTable(
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
      settings: TableWriterSettings[K, V],
      kvtClientConfig: KeyValueTableClientConfiguration
  ): RIO[Scope, ZSink[Any, Throwable, (K, V), Nothing, Unit]] =
    table(tableName, kvtClientConfig)
      .map { table =>
        ZSink.foreach { (pair: (K, V)) =>
          val (k, v) = pair
          val put =
            new Put(settings.tableKey(k), settings.valueSerializer.serialize(v))
          ZIO.fromCompletableFuture(table.update(put))
        }
      }

  override def source[K, V](
      tableName: String,
      settings: TableReaderSettings[K, V],
      kvtClientConfig: KeyValueTableClientConfiguration
  ): RIO[Scope, ZStream[Any, Throwable, TableEntry[V]]] =
    table(tableName, kvtClientConfig).map { table =>
      val it = table
        .iterator()
        .maxIterationSize(settings.maxEntriesAtOnce)
        .all()
        .entries()
      ZStream.repeatZIOChunk {
        Task
          .fromCompletableFuture(it.getNext())
          .map {
            case null => Chunk.empty
            case el =>
              val res = el.getItems().asScala.map { tableEntry =>
                TableEntry(
                  tableEntry.getKey(),
                  tableEntry.getVersion(),
                  settings.valueSerializer
                    .deserialize(tableEntry.getValue())
                )
              }
              Chunk.fromArray(res.toArray)
          }
      }
    }

  def flow[K, V](
      tableName: String,
      settings: TableWriterSettings[K, V],
      kvtClientConfig: KeyValueTableClientConfiguration
  ): RIO[Scope, ZPipeline[Any, Throwable, (K, V), TableEntry[V]]] =
    table(tableName, kvtClientConfig)
      .map { table =>
        ZPipeline.mapZIO { (pair: (K, V)) =>
          val (k, v) = pair
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
      settings: TableReaderSettings[K, V],
      kvtClientConfig: KeyValueTableClientConfiguration
  ): RIO[Scope, ZPipeline[Any, Throwable, K, Option[TableEntry[V]]]] =
    table(tableName, kvtClientConfig)
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
      URIO.attemptBlocking(fac.close()).ignore

    for {
      clientConfig <- ZIO.service[ClientConfig]
      clientFactory <- ZIO
        .acquireRelease(acquire(clientConfig))(release)
    } yield PravegaTable(clientFactory)
  }

  def fromScope(
      scope: String
  ): ZLayer[ClientConfig & Scope, Throwable, PravegaTableService] = ZLayer(
    service(
      scope
    )
  )

}
