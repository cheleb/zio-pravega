package zio.pravega

import zio._
import zio.stream._
import io.pravega.client.KeyValueTableFactory
import io.pravega.client.tables.KeyValueTableClientConfiguration

trait PravegaTableService {
  def sink[K, V](
      tableName: String,
      settings: TableWriterSettings[K, V]
  ): RIO[Scope, ZSink[Any, Throwable, (K, V), Nothing, Unit]]

  def source[K, V](
      readerGroupName: String,
      settings: TableReaderSettings[K, V]
  ): Task[ZStream[Any, Throwable, (K, V)]]
}

final case class PravegaTable(
    keyValueTableFactory: KeyValueTableFactory,
    kvtClientConfig: KeyValueTableClientConfiguration
) extends PravegaTableService {

  override def sink[K, V](
      tableName: String,
      settings: TableWriterSettings[K, V]
  ): RIO[Scope, ZSink[Any, Throwable, (K, V), Nothing, Unit]] = {
    val table =
      keyValueTableFactory.forKeyValueTable(tableName, kvtClientConfig)
    println(table)
    ???
  }

  override def source[K, V](
      readerGroupName: String,
      settings: TableReaderSettings[K, V]
  ): Task[ZStream[Any, Throwable, (K, V)]] = ???

}
