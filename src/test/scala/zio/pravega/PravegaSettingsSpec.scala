package zio.pravega

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.must.Matchers
import io.pravega.client.stream.impl.UTF8StringSerializer

import io.pravega.client.ClientConfig
import java.net.URI

import scala.concurrent.duration.DurationInt

import scala.language.postfixOps

class PravegaSettingsSpec extends AnyWordSpec with Matchers {

  "Prvega settings builders" must {

    "Allows reader to set client config" in {

      val clientConfig = ClientConfig
        .builder()
        .controllerURI(new URI("tcp://localhost:9090:"))
        .build()

      val readerSettings = ReaderSettingsBuilder()
        .withClientConfig(clientConfig)
        .clientConfigBuilder(_.enableTlsToController(true))
        .withSerializer(new UTF8StringSerializer)

      readerSettings.clientConfig.isEnableTlsToController() mustBe true

    }

    "Allows writer to set client config" in {

      val clientConfig = ClientConfig
        .builder()
        .controllerURI(new URI("tcp://localhost:9090:"))
        .build()

      val writterSettings = WriterSettingsBuilder()
        .withClientConfig(clientConfig)
        .clientConfigBuilder(_.enableTlsToController(true))
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
        .clientConfigBuilder(_.enableTlsToController(true))
        .readerConfigBuilder(_.bufferSize(1024))
        .withReaderId("dummy")
        .withTimeout(10 seconds)
        .withSerializer(new UTF8StringSerializer)

      readerSettings.clientConfig.isEnableTlsToController() mustBe true
      readerSettings.readerConfig.getBufferSize() mustEqual 1024
      readerSettings.readerId mustEqual Some("dummy")

    }
  }

}
