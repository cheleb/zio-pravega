package zio.pravega

import zio._
import zio.stream._

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

final case class PravegaTable()
