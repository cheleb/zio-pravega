package zio.pravega

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.must.Matchers
import io.pravega.client.stream.impl.UTF8StringSerializer

import scala.concurrent.duration.DurationInt

import scala.language.postfixOps
import io.pravega.client.tables.TableKey
import java.nio.ByteBuffer

class PravegaSettingsSpec extends AnyWordSpec with Matchers {

  private val clientConfig = PravegaClientConfig.builder
    .enableTlsToController(true)
    .build()

  "Pravega stream settings builders" must {

    "Allows reader to set client config" in {

      val readerSettings = ReaderSettingsBuilder()
        .withClientConfig(clientConfig)
        .withReaderId("A hard coded reader id")
        .withSerializer(new UTF8StringSerializer)

      readerSettings.clientConfig.isEnableTlsToController() mustBe true

    }

    "Allows writer to set client config" in {

      val writterSettings = WriterSettingsBuilder()
        .withClientConfig(clientConfig)
        .eventWriterConfigBuilder(_.enableConnectionPooling(true))
        .withMaximumInflightMessages(100)
        .withKeyExtractor((str: String) => str.substring(0, 2))
        .withSerializer(new UTF8StringSerializer)

      writterSettings.clientConfig.isEnableTlsToController() mustBe true
      writterSettings.eventWriterConfig.isEnableConnectionPooling() mustBe true
      writterSettings.maximumInflightMessages === 100

    }

    "Allows client config customisation" in {

      val readerSettings = ReaderSettingsBuilder()
        .withClientConfig(clientConfig)
        .readerConfigBuilder(_.bufferSize(1024))
        .withReaderId("dummy")
        .withTimeout(10 seconds)
        .withSerializer(new UTF8StringSerializer)

      readerSettings.clientConfig.isEnableTlsToController() mustBe true
      readerSettings.readerConfig.getBufferSize() mustEqual 1024
      readerSettings.readerId mustEqual Some("dummy")

    }
  }

  "Pravega table setting builder" must {
    "Allow table reader settings" in {
      val tableReaderSettings = TableReaderSettingsBuilder(
        new UTF8StringSerializer,
        new UTF8StringSerializer
      )
        .withClientConfig(clientConfig)
        .withMaximumInflightMessages(10)
        .keyValueTableClientConfigurationBuilder(_.backoffMultiple(2))
        .withKeyExtractor(str => new TableKey(ByteBuffer.wrap(str.getBytes())))
        .withMaxEntriesAtOnce(1000)
        .build()

      tableReaderSettings.maximumInflightMessages mustEqual 10
      tableReaderSettings.maxEntriesAtOnce mustEqual 1000

      val tableReaderSettingsDefault = TableReaderSettingsBuilder(
        new UTF8StringSerializer,
        new UTF8StringSerializer
      )
        .withClientConfig(clientConfig)
        .withMaximumInflightMessages(10)
        .keyValueTableClientConfigurationBuilder(_.backoffMultiple(2))
        .withMaxEntriesAtOnce(1000)
        .build()

      tableReaderSettingsDefault.maximumInflightMessages mustEqual 10
      tableReaderSettingsDefault.maxEntriesAtOnce mustEqual 1000

    }
    "Allow table writter settings" in {
      val tableWritterSettings = TableWriterSettingsBuilder(
        new UTF8StringSerializer,
        new UTF8StringSerializer
      )
        .withClientConfig(clientConfig)
        .withMaximumInflightMessages(100)
        .withKeyExtractor(str => new TableKey(ByteBuffer.wrap(str.getBytes())))
        .keyValueTableClientConfigurationBuilder(_.retryAttempts(3))
        .build()

      tableWritterSettings.clientConfig.isEnableTlsToController mustBe true
      tableWritterSettings.maximumInflightMessages mustEqual 100

      val tableWritterSettingsDefaultExtractor = TableWriterSettingsBuilder(
        new UTF8StringSerializer,
        new UTF8StringSerializer
      )
        .withClientConfig(clientConfig)
        .withMaximumInflightMessages(100)
        .keyValueTableClientConfigurationBuilder(_.retryAttempts(3))
        .build()

      tableWritterSettingsDefaultExtractor.clientConfig.isEnableTlsToController mustBe true
      tableWritterSettingsDefaultExtractor.maximumInflightMessages mustEqual 100

    }
  }

}
