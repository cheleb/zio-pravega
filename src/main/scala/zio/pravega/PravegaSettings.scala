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
import io.pravega.client.tables.TableKey
import com.typesafe.config.ConfigFactory

private[pravega] abstract class WithClientConfig(
    config: Config,
    clientConfig: Option[ClientConfig] = None,
    clientConfigCustomization: Option[
      ClientConfigBuilder => ClientConfigBuilder
    ] = None
) {

  protected def handleClientConfig(): ClientConfig = {
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

class PravegaClientConfigBuilder(
    config: Config,
    clientConfig: Option[ClientConfig] = None,
    clientConfigCustomization: Option[
      ClientConfigBuilder => ClientConfigBuilder
    ] = None
) extends WithClientConfig(config, clientConfig, clientConfigCustomization) {
  def build() = handleClientConfig()
}

object PravegaClientConfigBuilder {
  val configPath = "zio.pravega.defaults"

  def apply(
      clientConfig: Option[ClientConfig] = None,
      clientConfigCustomization: Option[
        ClientConfigBuilder => ClientConfigBuilder
      ] = None
  ) = new PravegaClientConfigBuilder(
    ConfigFactory.load().getConfig(configPath),
    clientConfig,
    clientConfigCustomization
  )
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
    timeout: Duration,
    readerId: Option[String]
) extends WithClientConfig(config, clientConfig, clientConfigCustomization) {

  def withClientConfig(clientConfig: ClientConfig): ReaderSettingsBuilder =
    copy(clientConfig = Some(clientConfig))

  def clientConfigBuilder(
      clientConfigCustomization: ClientConfigBuilder => ClientConfigBuilder
  ): ReaderSettingsBuilder =
    copy(clientConfigCustomization = Some(clientConfigCustomization))

  def readerConfigBuilder(
      f: ReaderConfigBuilder => ReaderConfigBuilder
  ): ReaderSettingsBuilder =
    copy(readerConfigCustomizer = Some(f))

  def withReaderId(id: String): ReaderSettingsBuilder =
    copy(readerId = Some(id))

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
      timeout: Duration = timeout,
      readerId: Option[String] = readerId
  ) =
    new ReaderSettingsBuilder(
      config,
      clientConfig,
      clientConfigCustomization,
      readerConfigBuilder,
      readerConfigCustomizer,
      timeout,
      readerId
    )
}

/** Reader settings that must be provided to @see
  * [[zio.pravega.PravegaStream#stream]]
  *
  * Built with @see [[ReaderSettingsBuilder]]
  *
  * @param clientConfig
  * @param readerConfig
  * @param timeout
  * @param serializer
  * @param readerId
  * @tparam Message
  */
class ReaderSettings[Message] private[pravega] (
    val clientConfig: ClientConfig,
    val readerConfig: ReaderConfig,
    val timeout: Long,
    val serializer: Serializer[Message],
    val readerId: Option[String]
)

object ReaderSettingsBuilder {

  val configPath = "zio.pravega.reader"

  def apply(): ReaderSettingsBuilder =
    apply(ConfigFactory.load().getConfig(configPath))

  /** Create settings from a configuration with the same layout as the default
    * configuration `zio.pravega.reader`.
    */
  def apply[Message](implicit config: Config): ReaderSettingsBuilder = {

    import ConfigHelper._

    val readerBasicSetting = new ReaderBasicSetting()
    extractString("group-name")(readerBasicSetting.withGroupName)
    extractDuration("timeout")(readerBasicSetting.withTimeout)

    val readerConfigBuilder = ConfigHelper.buildReaderConfig(config)

    new ReaderSettingsBuilder(
      config,
      None,
      None,
      readerConfigBuilder,
      None,
      readerBasicSetting.timeout,
      None
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
    apply(ConfigFactory.load().getConfig(configPath))

  /** Create settings from a configuration with the same layout as the default
    * configuration `zio.pravega`.
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

class TableReaderSettingsBuilder[K, V](
    config: Config,
    keySerializer: Serializer[K],
    valueSerializer: Serializer[V],
    tableKeyExtractor: Option[K => TableKey],
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
  ): TableReaderSettingsBuilder[K, V] =
    copy(keyValueTableClientConfigurationBuilderCustomizer = Some(f))

  def withClientConfig(clientConfig: ClientConfig) =
    copy(clientConfig = Some(clientConfig))

  def withMaximumInflightMessages(i: Int): TableReaderSettingsBuilder[K, V] =
    copy(maximumInflightMessages = i)

  def withMaxEntriesAtOnce(i: Int): TableReaderSettingsBuilder[K, V] =
    copy(maxEntriesAtOnce = i)

  def clientConfigBuilder(
      clientConfigModifier: ClientConfigBuilder => ClientConfigBuilder
  ): TableReaderSettingsBuilder[K, V] =
    copy(clientConfigModifier = Some(clientConfigModifier))

  private def copy(
      clientConfig: Option[ClientConfig] = clientConfig,
      tableKeyExtractor: Option[K => TableKey] = tableKeyExtractor,
      clientConfigModifier: Option[ClientConfigBuilder => ClientConfigBuilder] =
        clientConfigModifier,
      keyValueTableClientConfigurationBuilderCustomizer: Option[
        KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder => KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder
      ] = keyValueTableClientConfigurationBuilderCustomizer,
      maximumInflightMessages: Int = maximumInflightMessages,
      maxEntriesAtOnce: Int = maxEntriesAtOnce
  ): TableReaderSettingsBuilder[K, V] =
    new TableReaderSettingsBuilder(
      config,
      keySerializer,
      valueSerializer,
      tableKeyExtractor,
      clientConfig,
      clientConfigModifier,
      keyValueTableClientConfigurationBuilder,
      keyValueTableClientConfigurationBuilderCustomizer,
      maximumInflightMessages,
      maxEntriesAtOnce
    )

  /** Build the settings.
    */
  def withKeyExtractor(
      extractor: K => TableKey
  ): TableReaderSettingsBuilder[K, V] =
    copy(tableKeyExtractor = Some(extractor))

  def build(): TableReaderSettings[K, V] = {

    keyValueTableClientConfigurationBuilderCustomizer.foreach(
      _(keyValueTableClientConfigurationBuilder)
    )

    val clientConfig = keyValueTableClientConfigurationBuilder.build()

    new TableReaderSettings[K, V](
      handleClientConfig(),
      clientConfig,
      valueSerializer,
      tableKeyExtractor.getOrElse { k =>
        new TableKey(keySerializer.serialize(k))
      },
      maximumInflightMessages,
      maxEntriesAtOnce
    )
  }

}

object TableReaderSettingsBuilder {

  val configPath = "zio.pravega.table"

  def apply[K, V](
      keySerializer: Serializer[K],
      valueSerializer: Serializer[V]
  ): TableReaderSettingsBuilder[K, V] =
    apply(
      ConfigFactory.load().getConfig(configPath),
      keySerializer,
      valueSerializer
    )

  /** Create settings from a configuration with the same layout as the default
    * configuration `zio.pravega`.
    */
  def apply[K, V](
      config: Config,
      keySerializer: Serializer[K],
      valueSerializer: Serializer[V]
  ): TableReaderSettingsBuilder[K, V] =
    new TableReaderSettingsBuilder(
      config,
      keySerializer,
      valueSerializer,
      None,
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
    keySerializer: Serializer[K],
    combine: (V, V) => V,
    valueSerializer: Serializer[V],
    tableKeyExtractor: Option[K => TableKey],
    clientConfig: Option[ClientConfig] = None,
    clientConfigCustomization: Option[
      ClientConfigBuilder => ClientConfigBuilder
    ] = None,
    keyValueTableClientConfigurationBuilder: KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder,
    keyValueTableClientConfigurationBuilderCustomizer: Option[
      KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder => KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder
    ] = None,
    maximumInflightMessages: Int
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

  private def copy(
      tableKeyExtractor: Option[K => TableKey] = tableKeyExtractor,
      clientConfig: Option[ClientConfig] = clientConfig,
      clientConfigCustomization: Option[
        ClientConfigBuilder => ClientConfigBuilder
      ] = clientConfigCustomization,
      keyValueTableClientConfigurationBuilderCustomizer: Option[
        KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder => KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder
      ] = keyValueTableClientConfigurationBuilderCustomizer,
      maximumInflightMessages: Int = maximumInflightMessages
  ): TableWriterSettingsBuilder[K, V] =
    new TableWriterSettingsBuilder(
      config,
      keySerializer,
      combine,
      valueSerializer,
      tableKeyExtractor,
      clientConfig,
      clientConfigCustomization,
      keyValueTableClientConfigurationBuilder,
      keyValueTableClientConfigurationBuilderCustomizer,
      maximumInflightMessages
    )

  /** Build the settings.
    */
  def withKeyExtractor(
      extractor: K => TableKey
  ): TableWriterSettingsBuilder[K, V] =
    copy(tableKeyExtractor = Some(extractor))

  def build(): TableWriterSettings[K, V] = {

    keyValueTableClientConfigurationBuilderCustomizer.foreach(
      _(keyValueTableClientConfigurationBuilder)
    )

    val eventWriterConfig = keyValueTableClientConfigurationBuilder.build()
    new TableWriterSettings[K, V](
      handleClientConfig(),
      eventWriterConfig,
      combine,
      valueSerializer,
      tableKeyExtractor.getOrElse(k =>
        new TableKey(keySerializer.serialize(k))
      ),
      maximumInflightMessages
    )
  }

}

object TableWriterSettingsBuilder {

  private val configPath = "zio.pravega.table"

  /** Create settings from a configuration with the same layout as the default
    * configuration `zio.pravega`.
    */
  def apply[K, V](
      keySerializer: Serializer[K],
      valueSerializer: Serializer[V]
  )(combine: (V, V) => V): TableWriterSettingsBuilder[K, V] =
    apply(
      ConfigFactory.load().getConfig(configPath),
      keySerializer,
      valueSerializer
    )(combine)

  def apply[K, V](
      config: Config,
      keySerializer: Serializer[K],
      valueSerializer: Serializer[V]
  )(combine: (V, V) => V): TableWriterSettingsBuilder[K, V] =
    new TableWriterSettingsBuilder(
      config,
      keySerializer,
      combine,
      valueSerializer,
      None,
      None,
      None,
      tableClientConfiguration(config),
      None,
      config.getInt("maximum-inflight-messages")
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
    var timeout: Duration = Duration.ofSeconds(5)
) {
  def withGroupName(name: String) = groupName = Some(name)
  def withTimeout(t: Duration) = timeout = t
}

/** Writer settings that must be provided to @see Sink
  * [[zio.pravega.PravegaStream#sink]]
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
  * [[zio.pravega.PravegaStream.sink]] Built with @see [[WriterSettingsBuilder]]
  */
class TableWriterSettings[K, V](
    clientConfig: ClientConfig,
    keyValueTableClientConfiguration: KeyValueTableClientConfiguration,
    val combine: (V, V) => V,
    valueSerializer: Serializer[V],
    tableKey: K => TableKey,
    maximumInflightMessages: Int
) extends TableSettings(
      clientConfig,
      valueSerializer,
      tableKey,
      keyValueTableClientConfiguration,
      maximumInflightMessages
    )

class TableReaderSettings[K, V](
    clientConfig: ClientConfig,
    keyValueTableClientConfiguration: KeyValueTableClientConfiguration,
    valueSerializer: Serializer[V],
    tableKey: K => TableKey,
    maximumInflightMessages: Int,
    val maxEntriesAtOnce: Int
) extends TableSettings(
      clientConfig,
      valueSerializer,
      tableKey,
      keyValueTableClientConfiguration,
      maximumInflightMessages
    )

protected abstract class TableSettings[K, V](
    val clientConfig: ClientConfig,
    val valueSerializer: Serializer[V],
    val tableKey: K => TableKey,
    val keyValueTableClientConfiguration: KeyValueTableClientConfiguration,
    val maximumInflightMessages: Int
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
    extractString("controller-uri") { uri =>
      builder.controllerURI(new URI(uri))
    }
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

  def extractString(
      path: String
  )(f: String => Any)(implicit config: Config): Unit =
    if (config.hasPath(path))
      f(config.getString(path)): Unit
  def extractBoolean(
      path: String
  )(f: Boolean => Any)(implicit config: Config): Unit =
    if (config.hasPath(path))
      f(config.getBoolean(path)): Unit
  def extractInt(path: String)(f: Int => Any)(implicit config: Config): Unit =
    if (config.hasPath(path))
      f(config.getInt(path)): Unit
  def extractLong(path: String)(f: Long => Any)(implicit config: Config): Unit =
    if (config.hasPath(path))
      f(config.getLong(path)): Unit
  def extractDuration(
      path: String
  )(f: Duration => Unit)(implicit config: Config): Unit =
    if (config.hasPath(path))
      f(config.getDuration(path))
}
