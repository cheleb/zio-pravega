package zio.pravega

import zio._
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._

import io.pravega.client.tables.KeyValueTableConfiguration
import io.pravega.client.stream.ReaderGroupConfig

object AdminSpec extends SharedPravegaContainerSpec("admin") {

  override def spec: Spec[Environment with TestEnvironment with Scope, Any] =
    suite("Admin")(
      namespaceSuite,
      streamSuite,
      groupsSuite,
      tableSuite
    ).provide(Scope.default, PravegaAdmin.live(clientConfig)) @@ sequential

  def namespaceSuite = suite("Scopes")(
    test("Scope created once")(
      PravegaAdmin
        .createScope(aScope)
        .map(once => assert(once)(isTrue))
    ),
    test("Scope skip twice")(
      PravegaAdmin
        .createScope(aScope)
        .map(twice => assert(twice)(isFalse))
    )
  ) @@ sequential

  def streamSuite =
    suite("Streams")(
      test("Stream created once")(
        PravegaAdmin
          .createStream(
            aScope,
            "stream",
            streamConfig(2)
          )
          .map(once => assert(once)(isTrue))
      ),
      test("Stream skip twice")(
        PravegaAdmin
          .createStream(
            aScope,
            "stream",
            streamConfig(2)
          )
          .map(twice => assert(twice)(isFalse))
      )
    ) @@ sequential

  def groupsSuite = suite("Groups")(
    test("Group created")(
      PravegaAdmin
        .createReaderGroup(
          aScope,
          "group",
          "stream"
        )
        .map(once => assert(once)(isTrue))
    ),
    test("Group created")(
      PravegaAdmin
        .createReaderGroup(
          aScope,
          "group2",
          ReaderGroupConfig.builder
            .automaticCheckpointIntervalMillis(1000),
          "stream"
        )
        .map(once => assert(once)(isTrue))
    ),
    test("Open")(
      PravegaAdmin
        .openReaderGroup(
          aScope,
          "group"
        )
        .map(_ => assertCompletes)
    )
  )

  def tableSuite = {
    val tableConfig = KeyValueTableConfiguration
      .builder()
      .partitionCount(2)
      .primaryKeyLength(4)
      .build()
    suite("Tables")(
      test("Table created once")(
        PravegaAdmin
          .createTable(
            aScope,
            "table",
            tableConfig
          )
          .map(once => assert(once)(isTrue))
      ),
      test("Table skip twice")(
        PravegaAdmin
          .createTable(
            aScope,
            "table",
            tableConfig
          )
          .map(twice => assert(twice)(isFalse))
      )
    ) @@ sequential
  }

}
/*


      test(s"Create group $groupName1 && $groupName2")(
        ZIO
          .scoped(
            PravegaAdmin
              .createReaderGroup(
                pravegaScope,
                groupName1,
                pravegaStreamName1
              )
              .zipWith(
                PravegaAdmin.createReaderGroup(
                  pravegaScope,
                  groupName2,
                  ReaderGroupConfig.builder
                    .automaticCheckpointIntervalMillis(1000),
                  pravegaStreamName2
                )
              )(_ && _)
          )
          .map(once => assert(once)(isTrue))
      ),
      test("Create reader buggy")(
        ZIO
          .scoped(
            PravegaAdmin.createReaderGroup(
              "zio-scope",
              "coco-buggy",
              pravegaStreamName1
            ) *> PravegaAdmin.createReaderGroup(
              "zio-scope",
              "coco-buggy2",
              pravegaStreamName1
            )
          )
          .map(once => assert(once)(isTrue))
      )
    ) @@ sequential

  def adminSuite2(pravegaScope: String, pravegaTableName: String) =
    suite("Pravega KVP Table")(
      test(s"Create table $pravegaTableName")(
        ZIO
          .scoped(
            PravegaAdmin.createTable(
              pravegaTableName,
              tableConfig,
              pravegaScope
            )
          )
          .map(created => assert(created)(isTrue))
      )
    )

  def adminCleanSpec =
    suite("Admin clean")(
      test("Set reader offline")(
        ZIO.debug("""
        🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥
        🔥   Pravega container noisy log expected below  🔥
        🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥""") *>
          ZIO
            .scoped(
              ZIO
                .attemptBlocking(
                  EventStreamClientFactory
                    .withScope(
                      "zio-scope",
                      ClientConfig
                        .builder()
                        .build()
                    )
                )
                .withFinalizerAuto
                .map(
                  _.createReader( // This reader is intentionaly not close to leak
                    UUID.randomUUID().toString,
                    "coco-buggy",
                    new UTF8StringSerializer,
                    ReaderConfig.builder().build()
                  )
                ) *> PravegaAdmin
                .readerOffline("zio-scope", "coco-buggy")
            )
            .map(n => assert(n)(equalTo(1)))
      )
    ) @@ sequential

}
 */
