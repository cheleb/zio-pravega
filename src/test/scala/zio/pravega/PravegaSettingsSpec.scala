package zio.pravega

import zio._
import io.pravega.client.stream.impl.UTF8StringSerializer

import scala.language.postfixOps
import io.pravega.client.tables.TableKey
import java.nio.ByteBuffer
import zio.pravega.serder.UTF8StringScalaDeserializer

class PravegaSettingsSpec extends munit.FunSuite {

  private val clientConfig = PravegaClientConfig.builder.enableTlsToController(true).build()

  test("Allows writer to set client config") {

    val writterSettings = WriterSettingsBuilder()
      .eventWriterConfigBuilder(_.enableConnectionPooling(true))
      .withMaximumInflightMessages(100)
      .withKeyExtractor((str: String) => str.substring(0, 2))
      .withSerializer(new UTF8StringSerializer)

    assert(writterSettings.eventWriterConfig.isEnableConnectionPooling(), "Connection pooling should be enabled")
    assertEquals(writterSettings.maximumInflightMessages, 100)

    test("Allows client config customisation") {

      val readerSettings = ReaderSettingsBuilder()
        .readerConfigBuilder(_.bufferSize(1024))
        .withReaderId("dummy")
        .withTimeout(10 seconds)
        .withDeserializer(UTF8StringScalaDeserializer)

      assertEquals(readerSettings.readerConfig.getBufferSize(), 1024)
//      readerSettings.readerId mustEqual Some("dummy")

    }
  }

  test("Allow table reader settings") {
    val tableReaderSettings = TableReaderSettingsBuilder(new UTF8StringSerializer, new UTF8StringSerializer)
      .withMaximumInflightMessages(10)
      .withConfigurationCustomiser(_.backoffMultiple(2))
      .withKeyExtractor(str => new TableKey(ByteBuffer.wrap(str.getBytes("UTF-8"))))
      .withMaxEntriesAtOnce(1000)
      .build()

    val tableReaderSettingsDefault = TableReaderSettingsBuilder(new UTF8StringSerializer, new UTF8StringSerializer)
      .withMaximumInflightMessages(10)
      .withConfigurationCustomiser(_.backoffMultiple(2))
      .withMaxEntriesAtOnce(1000)
      .build()

    assertEquals(
      (
        tableReaderSettings.maximumInflightMessages,
        tableReaderSettings.maxEntriesAtOnce,
        tableReaderSettingsDefault.maximumInflightMessages,
        tableReaderSettingsDefault.maxEntriesAtOnce
      ),
      (10, 1000, 10, 1000)
    )

  }
  test("Allow table writter settings") {
    val tableWritterSettings = TableWriterSettingsBuilder(new UTF8StringSerializer, new UTF8StringSerializer)
      .withMaximumInflightMessages(100)
      .withKeyExtractor(str => new TableKey(ByteBuffer.wrap(str.getBytes("UTF-8"))))
      .withConfigurationCustomiser(_.retryAttempts(3))
      .build()

    val tableWritterSettingsDefaultExtractor =
      TableWriterSettingsBuilder(new UTF8StringSerializer, new UTF8StringSerializer)
        .withMaximumInflightMessages(100)
        .withConfigurationCustomiser(_.retryAttempts(3))
        .build()

    assertEquals(
      (
        tableWritterSettings.maximumInflightMessages,
        tableWritterSettingsDefaultExtractor.maximumInflightMessages
      ),
      (100, 100)
    )

  }

}
