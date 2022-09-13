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

object PravegaClientConfig {
  val configPath = "zio.pravega"

  def default: ClientConfig = builder.build()

  def builder: ClientConfigBuilder =
    ConfigHelper.builder(ConfigFactory.load().getConfig(configPath))

}

class ReaderSettingsBuilder(
    config: Config,
    readerConfigBuilder: ReaderConfigBuilder,
    readerConfigCustomizer: Option[ReaderConfigBuilder => ReaderConfigBuilder] =
      None,
    timeout: Duration,
    readerId: Option[String]
) {

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
      readerConfigBuilder.build(),
      timeout.toMillis,
      serializer,
      readerId
    )
  }

  private def copy(
      readerConfigBuilder: ReaderConfigBuilder = readerConfigBuilder,
      readerConfigCustomizer: Option[
        ReaderConfigBuilder => ReaderConfigBuilder
      ] = readerConfigCustomizer,
      timeout: Duration = timeout,
      readerId: Option[String] = readerId
  ) =
    new ReaderSettingsBuilder(
      config,
      readerConfigBuilder,
      readerConfigCustomizer,
      timeout,
      readerId
    )
}

/** Reader settings that must be provided to @see
  * [[zio.pravega.PravegaStreamService#stream]]
  *
  * Built with @see [[ReaderSettingsBuilder]]
  *
  * @param readerConfig
  * @param timeout
  * @param serializer
  * @param readerId
  * @tparam Message
  */
class ReaderSettings[Message] private[pravega] (
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
      readerConfigBuilder,
      None,
      readerBasicSetting.timeout,
      None
    )

  }
}

class WriterSettingsBuilder[Message](
    config: Config,
    eventWriterConfigBuilder: EventWriterConfigBuilder,
    eventWriterConfigCustomizer: Option[
      EventWriterConfigBuilder => EventWriterConfigBuilder
    ] = None,
    maximumInflightMessages: Int,
    keyExtractor: Option[Message => String]
) {

  def eventWriterConfigBuilder(
      f: EventWriterConfigBuilder => EventWriterConfigBuilder
  ): WriterSettingsBuilder[Message] =
    copy(eventWriterConfigCustomizer = Some(f))

  def withMaximumInflightMessages(i: Int): WriterSettingsBuilder[Message] =
    copy(maximumInflightMessages = i)

  def withKeyExtractor(
      keyExtractor: Message => String
  ): WriterSettingsBuilder[Message] =
    copy(keyExtractor = Some(keyExtractor))

  private def copy(
      eventWriterConfigCustomizer: Option[
        EventWriterConfigBuilder => EventWriterConfigBuilder
      ] = eventWriterConfigCustomizer,
      maximumInflightMessages: Int = maximumInflightMessages,
      keyExtractor: Option[Message => String] = keyExtractor
  ): WriterSettingsBuilder[Message] =
    new WriterSettingsBuilder(
      config,
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
    configurationBuilder: KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder,
    configurationCustomizer: Option[
      KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder => KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder
    ] = None,
    maximumInflightMessages: Int,
    maxEntriesAtOnce: Int
) {
  def withConfigurationCustomiser(
      f: KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder => KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder
  ): TableReaderSettingsBuilder[K, V] =
    copy(configurationCustomizer = Some(f))

  def withMaximumInflightMessages(i: Int): TableReaderSettingsBuilder[K, V] =
    copy(maximumInflightMessages = i)

  def withMaxEntriesAtOnce(i: Int): TableReaderSettingsBuilder[K, V] =
    copy(maxEntriesAtOnce = i)

  private def copy(
      tableKeyExtractor: Option[K => TableKey] = tableKeyExtractor,
      configurationCustomizer: Option[
        KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder => KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder
      ] = configurationCustomizer,
      maximumInflightMessages: Int = maximumInflightMessages,
      maxEntriesAtOnce: Int = maxEntriesAtOnce
  ): TableReaderSettingsBuilder[K, V] =
    new TableReaderSettingsBuilder(
      config,
      keySerializer,
      valueSerializer,
      tableKeyExtractor,
      configurationBuilder,
      configurationCustomizer,
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

    configurationCustomizer.foreach(
      _(configurationBuilder)
    )

    val clientConfig = configurationBuilder.build()

    new TableReaderSettings[K, V](
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
    valueSerializer: Serializer[V],
    tableKeyExtractor: Option[K => TableKey],
    configurationBuilder: KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder,
    configurationCustomizer: Option[
      KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder => KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder
    ] = None,
    maximumInflightMessages: Int
) {

  def withConfigurationCustomiser(
      f: KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder => KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder
  ): TableWriterSettingsBuilder[K, V] =
    copy(configurationCustomizer = Some(f))

  def withMaximumInflightMessages(i: Int): TableWriterSettingsBuilder[K, V] =
    copy(maximumInflightMessages = i)

  private def copy(
      tableKeyExtractor: Option[K => TableKey] = tableKeyExtractor,
      configurationCustomizer: Option[
        KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder => KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder
      ] = configurationCustomizer,
      maximumInflightMessages: Int = maximumInflightMessages
  ): TableWriterSettingsBuilder[K, V] =
    new TableWriterSettingsBuilder(
      config,
      keySerializer,
      valueSerializer,
      tableKeyExtractor,
      configurationBuilder,
      configurationCustomizer,
      maximumInflightMessages
    )

  /** Build the settings.
    */
  def withKeyExtractor(
      extractor: K => TableKey
  ): TableWriterSettingsBuilder[K, V] =
    copy(tableKeyExtractor = Some(extractor))

  def build(): TableWriterSettings[K, V] = {

    configurationCustomizer.foreach(
      _(configurationBuilder)
    )

    val eventWriterConfig = configurationBuilder.build()
    new TableWriterSettings[K, V](
      eventWriterConfig,
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
  ): TableWriterSettingsBuilder[K, V] =
    apply(
      ConfigFactory.load().getConfig(configPath),
      keySerializer,
      valueSerializer
    )

  def apply[K, V](
      config: Config,
      keySerializer: Serializer[K],
      valueSerializer: Serializer[V]
  ): TableWriterSettingsBuilder[K, V] =
    new TableWriterSettingsBuilder(
      config,
      keySerializer,
      valueSerializer,
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
  def withGroupName(name: String): Unit = groupName = Some(name)
  def withTimeout(t: Duration): Unit = timeout = t
}

/** Writer settings that must be provided to @see Sink
  * [[zio.pravega.PravegaStreamService#sink]]
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
    val eventWriterConfig: EventWriterConfig,
    val serializer: Serializer[Message],
    val keyExtractor: Option[Message => String],
    val maximumInflightMessages: Int
)

/** Table Writer settings that must be provided to @see Sink
  * [[zio.pravega.PravegaStreamService.sink]] Built with @see
  * [[WriterSettingsBuilder]]
  */
class TableWriterSettings[K, V](
    keyValueTableClientConfiguration: KeyValueTableClientConfiguration,
    valueSerializer: Serializer[V],
    tableKey: K => TableKey,
    maximumInflightMessages: Int
) extends TableSettings(
      valueSerializer,
      tableKey,
      keyValueTableClientConfiguration,
      maximumInflightMessages
    )

class TableReaderSettings[K, V](
    keyValueTableClientConfiguration: KeyValueTableClientConfiguration,
    valueSerializer: Serializer[V],
    tableKey: K => TableKey,
    maximumInflightMessages: Int,
    val maxEntriesAtOnce: Int
) extends TableSettings(
      valueSerializer,
      tableKey,
      keyValueTableClientConfiguration,
      maximumInflightMessages
    )

protected abstract class TableSettings[K, V](
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

  def builder(config: Config): ClientConfigBuilder = {
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
    builder
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
