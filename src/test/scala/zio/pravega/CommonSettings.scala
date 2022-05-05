package zio.pravega

import io.pravega.client.stream.impl.UTF8StringSerializer
import io.pravega.client.tables.KeyValueTableClientConfiguration

object CommonSettings {
  val writterSettings =
    WriterSettingsBuilder()
      .withSerializer(new UTF8StringSerializer)

  val readerSettings =
    ReaderSettingsBuilder()
      .withSerializer(new UTF8StringSerializer)

  val tableWriterSettings = TableWriterSettingsBuilder(
    new UTF8StringSerializer,
    new UTF8StringSerializer
  )
    .build()

  val tableReaderSettings = TableReaderSettingsBuilder(
    new UTF8StringSerializer,
    new UTF8StringSerializer
  )
    .build()

  val clientConfig = writterSettings.clientConfig

  val kvtClientConfig: KeyValueTableClientConfiguration =
    KeyValueTableClientConfiguration.builder().build()
}
