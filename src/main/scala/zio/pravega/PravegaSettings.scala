package zio.pravega

import java.net.URI
import java.time.Duration
import com.typesafe.config.Config
import io.pravega.client.stream.{EventWriterConfig, ReaderConfig, Serializer}
import io.pravega.client.ClientConfig
import io.pravega.client.ClientConfig.ClientConfigBuilder
import io.pravega.client.stream.EventWriterConfig.EventWriterConfigBuilder
import io.pravega.client.stream.ReaderConfig.ReaderConfigBuilder
import io.pravega.client.tables.KeyValueTableClientConfiguration
import com.typesafe.config.ConfigFactory

private[pravega] abstract class WithClientConfig(
    config: Config,
    clientConfig: Option[ClientConfig] = None,
    clientConfigCustomization: Option[
      ClientConfigBuilder => ClientConfigBuilder
    ] = None
) {

  def handleClientConfig() = {
    val rawClientConfig =
      clientConfig.getOrElse(
        ConfigHelper.buildClientConfigFromTypeSafeConfig(config)
      )

    clientConfigCustomization match {
      case Some(cust) => cust(rawClientConfig.toBuilder).build()
      case _          => rawClientConfig
    }

  }

}

class ReaderSettingsBuilder(
    config: Config,
    clientConfig: Option[ClientConfig] = None,
    clientConfigCustomization: Option[
      ClientConfigBuilder => ClientConfigBuilder
    ] = None,
    readerConfigBuilder: ReaderConfigBuilder,
    readerConfigCustomizer: Option[ReaderConfigBuilder => ReaderConfigBuilder] =
      None,
    groupName: Option[String],
    timeout: Duration,
    readerId: Option[String]
) extends WithClientConfig(config, clientConfig, clientConfigCustomization) {

  def withClientConfig(clientConfig: ClientConfig): ReaderSettingsBuilder =
    copy(clientConfig = Some(clientConfig))

  def clientConfigBuilder(
      clientConfigCustomization: ClientConfigBuilder => ClientConfigBuilder
  ) =
    copy(clientConfigCustomization = Some(clientConfigCustomization))

  def readerConfigBuilder(f: ReaderConfigBuilder => ReaderConfigBuilder) =
    copy(readerConfigCustomizer = Some(f))

  def withGroupName(name: String): ReaderSettingsBuilder =
    copy(groupName = Some(name))
  def withReaderId(id: String): ReaderSettingsBuilder =
    copy(readerId = Some(id))

  // JavaDSL
  def withTimeout(timeout: java.time.Duration): ReaderSettingsBuilder =
    copy(timeout = timeout)
  // ScalaDSL
  def withTimeout(
      timeout: scala.concurrent.duration.Duration
  ): ReaderSettingsBuilder =
    copy(timeout = Duration.ofMillis(timeout.toMillis))

  def withSerializer[Message](
      serializer: Serializer[Message]
  ): ReaderSettings[Message] = {

    readerConfigCustomizer.foreach(_(readerConfigBuilder))

    new ReaderSettings[Message](
      handleClientConfig(),
      readerConfigBuilder.build(),
      groupName.getOrElse(
        throw new IllegalStateException("group-name is mandatory")
      ),
      timeout.toMillis,
      serializer,
      readerId
    )
  }

  private def copy(
      clientConfig: Option[ClientConfig] = clientConfig,
      clientConfigCustomization: Option[
        ClientConfigBuilder => ClientConfigBuilder
      ] = clientConfigCustomization,
      readerConfigBuilder: ReaderConfigBuilder = readerConfigBuilder,
      readerConfigCustomizer: Option[
        ReaderConfigBuilder => ReaderConfigBuilder
      ] = readerConfigCustomizer,
      groupName: Option[String] = groupName,
      timeout: Duration = timeout,
      readerId: Option[String] = readerId
  ) =
    new ReaderSettingsBuilder(
      config,
      clientConfig,
      clientConfigCustomization,
      readerConfigBuilder,
      readerConfigCustomizer,
      groupName,
      timeout,
      readerId
    )
}

/** Reader settings that must be provided to @see [[zio.pravega.Pravega#source]]
  *
  * Built with @see [[ReaderSettingsBuilder]]
  *
  * @param clientConfig
  * @param readerConfig
  * @param groupName
  * @param timeout
  * @param serializer
  * @param readerId
  * @tparam Message
  */
class ReaderSettings[Message] private[pravega] (
    val clientConfig: ClientConfig,
    val readerConfig: ReaderConfig,
    val groupName: String,
    val timeout: Long,
    val serializer: Serializer[Message],
    val readerId: Option[String]
)

object ReaderSettingsBuilder {
  val configPath = "zio.pravega.reader"

  def apply(): ReaderSettingsBuilder = apply(
    ConfigFactory.load.getConfig(ReaderSettingsBuilder.configPath)
  )

  /** Create settings from a configuration with the same layout as the default
    * configuration `akka.alpakka.pravega.reader`.
    */
  def apply(implicit config: Config): ReaderSettingsBuilder = {

    import ConfigHelper._

    val readerBasicSetting = new ReaderBasicSetting()
    extractString("group-name")(readerBasicSetting.withGroupName)
    extractDuration("timeout")(readerBasicSetting.withTimeout)
    extractString("reader-id")(readerBasicSetting.withReaderId)

    val readerConfigBuilder = ConfigHelper.buildReaderConfig(config)

    new ReaderSettingsBuilder(
      config,
      None,
      None,
      readerConfigBuilder,
      None,
      readerBasicSetting.groupName,
      readerBasicSetting.timeout,
      readerBasicSetting.readerId
    )

  }
}

class WriterSettingsBuilder[Message](
    config: Config,
    clientConfig: Option[ClientConfig] = None,
    clientConfigCustomization: Option[
      ClientConfigBuilder => ClientConfigBuilder
    ] = None,
    eventWriterConfigBuilder: EventWriterConfigBuilder,
    eventWriterConfigCustomizer: Option[
      EventWriterConfigBuilder => EventWriterConfigBuilder
    ] = None,
    maximumInflightMessages: Int,
    keyExtractor: Option[Message => String]
) extends WithClientConfig(config, clientConfig, clientConfigCustomization) {

  def eventWriterConfigBuilder(
      f: EventWriterConfigBuilder => EventWriterConfigBuilder
  ): WriterSettingsBuilder[Message] =
    copy(eventWriterConfigCustomizer = Some(f))

  def withClientConfig(clientConfig: ClientConfig) =
    copy(clientConfig = Some(clientConfig))

  def clientConfigBuilder(
      clientConfigCustomization: ClientConfigBuilder => ClientConfigBuilder
  ): WriterSettingsBuilder[Message] =
    copy(clientConfigCustomization = Some(clientConfigCustomization))

  def withMaximumInflightMessages(i: Int): WriterSettingsBuilder[Message] =
    copy(maximumInflightMessages = i)

  def withKeyExtractor(
      keyExtractor: Message => String
  ): WriterSettingsBuilder[Message] =
    copy(keyExtractor = Some(keyExtractor))

  private def copy(
      clientConfig: Option[ClientConfig] = clientConfig,
      clientConfigCustomization: Option[
        ClientConfigBuilder => ClientConfigBuilder
      ] = clientConfigCustomization,
      eventWriterConfigCustomizer: Option[
        EventWriterConfigBuilder => EventWriterConfigBuilder
      ] = eventWriterConfigCustomizer,
      maximumInflightMessages: Int = maximumInflightMessages,
      keyExtractor: Option[Message => String] = keyExtractor
  ): WriterSettingsBuilder[Message] =
    new WriterSettingsBuilder(
      config,
      clientConfig,
      clientConfigCustomization,
      eventWriterConfigBuilder,
      eventWriterConfigCustomizer,
      maximumInflightMessages,
      keyExtractor
    )

  /** Build the settings.
    */
  def withSerializer(
      serializer: Serializer[Message]
  ): WriterSettings[Message] = {

    eventWriterConfigCustomizer.foreach(_(eventWriterConfigBuilder))

    val eventWriterConfig = eventWriterConfigBuilder.build()
    new WriterSettings[Message](
      handleClientConfig(),
      eventWriterConfig,
      serializer,
      keyExtractor,
      maximumInflightMessages
    )
  }

}

object WriterSettingsBuilder {
  val configPath = "zio.pravega.writer"

  import ConfigHelper._

  def apply[Message](): WriterSettingsBuilder[Message] =
    apply(ConfigFactory.load.getConfig(WriterSettingsBuilder.configPath))

  /** Create settings from a configuration with the same layout as the default
    * configuration `akka.alpakka.pravega`.
    */
  def apply[Message](config: Config): WriterSettingsBuilder[Message] =
    new WriterSettingsBuilder(
      config,
      None,
      None,
      eventWriterConfig(config),
      None,
      config.getInt("maximum-inflight-messages"),
      None
    )

  private def eventWriterConfig(
      readerConfig: Config
  ): EventWriterConfigBuilder = {
    val builder = EventWriterConfig.builder()

    implicit val config = readerConfig.getConfig("config")

    extractBoolean("automatically-note-time")(builder.automaticallyNoteTime)
    extractInt("backoff-multiple")(builder.backoffMultiple)
    extractBoolean("enable-connection-pooling")(builder.enableConnectionPooling)
    extractInt("initial-backoff-millis")(builder.initialBackoffMillis)
    extractInt("retry-attempts")(builder.retryAttempts)
    extractLong("transaction-timeout-time")(builder.transactionTimeoutTime)

    builder
  }

}

class TableSettingsBuilder[K, V](
    config: Config,
    clientConfig: Option[ClientConfig] = None,
    clientConfigModifier: Option[ClientConfigBuilder => ClientConfigBuilder] =
      None,
    keyValueTableClientConfigurationBuilder: KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder,
    keyValueTableClientConfigurationBuilderCustomizer: Option[
      KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder => KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder
    ] = None,
    maximumInflightMessages: Int,
    maxEntriesAtOnce: Int
) extends WithClientConfig(config, clientConfig, clientConfigModifier) {
  def keyValueTableClientConfigurationBuilder(
      f: KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder => KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder
  ): TableSettingsBuilder[K, V] =
    copy(keyValueTableClientConfigurationBuilderCustomizer = Some(f))

  def withClientConfigModifier(clientConfig: ClientConfig) =
    copy(clientConfig = Some(clientConfig))

  def withMaximumInflightMessages(i: Int): TableSettingsBuilder[K, V] =
    copy(maximumInflightMessages = i)

  def withMaxEntriesAtOnce(i: Int): TableSettingsBuilder[K, V] =
    copy(maxEntriesAtOnce = i)

  def clientConfigBuilder(
      clientConfigModifier: ClientConfigBuilder => ClientConfigBuilder
  ): TableSettingsBuilder[K, V] =
    copy(clientConfigModifier = Some(clientConfigModifier))

  private def copy(
      clientConfig: Option[ClientConfig] = clientConfig,
      clientConfigModifier: Option[ClientConfigBuilder => ClientConfigBuilder] =
        clientConfigModifier,
      keyValueTableClientConfigurationBuilderCustomizer: Option[
        KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder => KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder
      ] = keyValueTableClientConfigurationBuilderCustomizer,
      maximumInflightMessages: Int = maximumInflightMessages,
      maxEntriesAtOnce: Int = maxEntriesAtOnce
  ): TableSettingsBuilder[K, V] =
    new TableSettingsBuilder(
      config,
      clientConfig,
      clientConfigModifier,
      keyValueTableClientConfigurationBuilder,
      keyValueTableClientConfigurationBuilderCustomizer,
      maximumInflightMessages,
      maxEntriesAtOnce
    )

  /** Build the settings.
    */
  def withKVSerializers(
      keySerializer: Serializer[K],
      valueSerializer: Serializer[V]
  ): TableSettings[K, V] = {

    keyValueTableClientConfigurationBuilderCustomizer.foreach(
      _(keyValueTableClientConfigurationBuilder)
    )

    val clientConfig = keyValueTableClientConfigurationBuilder.build()
    new TableSettings[K, V](
      handleClientConfig(),
      clientConfig,
      keySerializer,
      valueSerializer,
      maximumInflightMessages,
      maxEntriesAtOnce
    )
  }

}

object TableSettingsBuilder {
  val configPath = "akka.alpakka.pravega.table"

  /** Create settings from a configuration with the same layout as the default
    * configuration `akka.alpakka.pravega`.
    */
  def apply[K, V](config: Config): TableSettingsBuilder[K, V] =
    new TableSettingsBuilder(
      config,
      None,
      None,
      tableClientConfiguration(config),
      None,
      config.getInt("maximum-inflight-messages"),
      config.getInt("max-entries-at-once")
    )

  private def tableClientConfiguration(implicit config: Config) = {

    import ConfigHelper._

    val builder = KeyValueTableClientConfiguration.builder()

    extractInt("backoff-multiple")(builder.backoffMultiple)
    extractInt("initial-backoff-millis")(builder.initialBackoffMillis)
    extractInt("max-backoff-millis")(builder.maxBackoffMillis)
    extractInt("retry-attempts")(builder.retryAttempts)

    builder
  }
}

class TableWriterSettingsBuilder[K, V](
    config: Config,
    clientConfig: Option[ClientConfig] = None,
    clientConfigCustomization: Option[
      ClientConfigBuilder => ClientConfigBuilder
    ] = None,
    keyValueTableClientConfigurationBuilder: KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder,
    keyValueTableClientConfigurationBuilderCustomizer: Option[
      KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder => KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder
    ] = None,
    maximumInflightMessages: Int,
    maxEntriesAtOnce: Int
) extends WithClientConfig(config, clientConfig, clientConfigCustomization) {

  def keyValueTableClientConfigurationBuilder(
      f: KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder => KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder
  ): TableWriterSettingsBuilder[K, V] =
    copy(keyValueTableClientConfigurationBuilderCustomizer = Some(f))

  def withClientConfig(clientConfig: ClientConfig) =
    copy(clientConfig = Some(clientConfig))

  def clientConfigBuilder(
      clientConfigCustomization: ClientConfigBuilder => ClientConfigBuilder
  ): TableWriterSettingsBuilder[K, V] =
    copy(clientConfigCustomization = Some(clientConfigCustomization))

  def withMaximumInflightMessages(i: Int): TableWriterSettingsBuilder[K, V] =
    copy(maximumInflightMessages = i)

  def withMaxEntriesAtOnce(i: Int): TableWriterSettingsBuilder[K, V] =
    copy(maxEntriesAtOnce = i)

  private def copy(
      clientConfig: Option[ClientConfig] = clientConfig,
      clientConfigCustomization: Option[
        ClientConfigBuilder => ClientConfigBuilder
      ] = clientConfigCustomization,
      keyValueTableClientConfigurationBuilderCustomizer: Option[
        KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder => KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder
      ] = keyValueTableClientConfigurationBuilderCustomizer,
      maximumInflightMessages: Int = maximumInflightMessages,
      maxEntriesAtOnce: Int = maxEntriesAtOnce
  ): TableWriterSettingsBuilder[K, V] =
    new TableWriterSettingsBuilder(
      config,
      clientConfig,
      clientConfigCustomization,
      keyValueTableClientConfigurationBuilder,
      keyValueTableClientConfigurationBuilderCustomizer,
      maximumInflightMessages,
      maxEntriesAtOnce
    )

  /** Build the settings.
    */
  def withSerializers(
      keySerializer: Serializer[K],
      valueSerializer: Serializer[V]
  ): TableWriterSettings[K, V] = {

    keyValueTableClientConfigurationBuilderCustomizer.foreach(
      _(keyValueTableClientConfigurationBuilder)
    )

    val eventWriterConfig = keyValueTableClientConfigurationBuilder.build()
    new TableWriterSettings[K, V](
      handleClientConfig(),
      eventWriterConfig,
      keySerializer,
      valueSerializer,
      maximumInflightMessages,
      maxEntriesAtOnce
    )
  }

}

object TableWriterSettingsBuilder {
  val configPath = "akka.alpakka.pravega.table"

  /** Create settings from a configuration with the same layout as the default
    * configuration `akka.alpakka.pravega`.
    */
  def apply[K, V](config: Config): TableWriterSettingsBuilder[K, V] =
    new TableWriterSettingsBuilder(
      config,
      None,
      None,
      tableClientConfiguration(config),
      None,
      config.getInt("maximum-inflight-messages"),
      config.getInt("max-entries-at-once")
    )

  private def tableClientConfiguration(implicit config: Config) = {

    import ConfigHelper._

    val builder = KeyValueTableClientConfiguration.builder()

    extractInt("backoff-multiple")(builder.backoffMultiple)
    extractInt("initial-backoff-millis")(builder.initialBackoffMillis)
    extractInt("max-backoff-millis")(builder.maxBackoffMillis)
    extractInt("retry-attempts")(builder.retryAttempts)

    builder
  }
}

private[pravega] class ReaderBasicSetting(
    var groupName: Option[String] = None,
    var readerId: Option[String] = None,
    var timeout: Duration = Duration.ofSeconds(5)
) {
  def withGroupName(name: String) = groupName = Some(name)
  def withReaderId(name: String) = readerId = Some(name)
  def withTimeout(t: Duration) = timeout = t
}

/** Writer settings that must be provided to @see Sink
  * [[akka.stream.alpakka.pravega.scaladsl.Pravega#sink]] or @see Flow
  * [[akka.stream.alpakka.pravega.scaladsl.Pravega#flow]]
  *
  * Built with @see [[WriterSettingsBuilder]]
  *
  * @param clientConfig
  * @param eventWriterConfig
  * @param serializer
  * @param keyExtractor
  * @param maximumInflightMessages
  */
class WriterSettings[Message](
    val clientConfig: ClientConfig,
    val eventWriterConfig: EventWriterConfig,
    val serializer: Serializer[Message],
    val keyExtractor: Option[Message => String],
    val maximumInflightMessages: Int
)

/** Table Writer settings that must be provided to @see Sink
  * [[akka.stream.alpakka.pravega.scaladsl.Pravega#tableSink]] or @see Flow
  * [[akka.stream.alpakka.pravega.scaladsl.Pravega#tableflow]]
  *
  * Built with @see [[WriterSettingsBuilder]]
  */
class TableWriterSettings[K, V](
    clientConfig: ClientConfig,
    keyValueTableClientConfiguration: KeyValueTableClientConfiguration,
    keySerializer: Serializer[K],
    valueSerializer: Serializer[V],
    maximumInflightMessages: Int,
    maxEntriesAtOnce: Int
) extends TableSettings(
      clientConfig,
      keyValueTableClientConfiguration,
      keySerializer,
      valueSerializer,
      maximumInflightMessages,
      maxEntriesAtOnce
    )

class TableSettings[K, V](
    val clientConfig: ClientConfig,
    val keyValueTableClientConfiguration: KeyValueTableClientConfiguration,
    val keySerializer: Serializer[K],
    val valueSerializer: Serializer[V],
    val maximumInflightMessages: Int,
    val maxEntriesAtOnce: Int
)

private[pravega] object ConfigHelper {
  def buildReaderConfig(config: Config): ReaderConfigBuilder = {
    val builder = ReaderConfig
      .builder()

    implicit val c = config.getConfig("config")
    extractBoolean("disable-time-windows")(builder.disableTimeWindows)
    extractLong("initial-allocation-delay")(builder.initialAllocationDelay)

    builder
  }

  def buildClientConfigFromTypeSafeConfig(config: Config): ClientConfig = {
    val builder = ClientConfig.builder()

    implicit val c = config.getConfig("client-config")
    extractString("controller-uri")(uri => builder.controllerURI(new URI(uri)))
    extractBoolean("enable-tls-to-controller")(builder.enableTlsToController)
    extractBoolean("enable-tls-to-segment-store")(
      builder.enableTlsToSegmentStore
    )
    extractInt("max-connections-per-segment-store")(
      builder.maxConnectionsPerSegmentStore
    )
    extractString("trust-store")(builder.trustStore)
    extractBoolean("validate-host-name")(builder.validateHostName)

    builder.build()
  }

  def extractString[Builder](
      path: String
  )(f: String => Builder)(implicit config: Config): Unit =
    if (config.hasPath(path))
      f(config.getString(path)): Unit
  def extractBoolean[Builder](
      path: String
  )(f: Boolean => Builder)(implicit config: Config): Unit =
    if (config.hasPath(path))
      f(config.getBoolean(path)): Unit
  def extractInt[Builder](
      path: String
  )(f: Int => Builder)(implicit config: Config): Unit =
    if (config.hasPath(path))
      f(config.getInt(path)): Unit
  def extractLong[Builder](
      path: String
  )(f: Long => Builder)(implicit config: Config): Unit =
    if (config.hasPath(path))
      f(config.getLong(path)): Unit
  def extractDuration[Builder](
      path: String
  )(f: Duration => Builder)(implicit config: Config): Unit =
    if (config.hasPath(path))
      f(config.getDuration(path)): Unit
}
