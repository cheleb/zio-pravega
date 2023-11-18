package zio.pravega

import zio._
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._

import io.pravega.client.tables.KeyValueTableConfiguration
import io.pravega.client.stream.ReaderGroupConfig
import zio.pravega.admin._
import zio.stream.ZStream

/**
 * This test suite is a bit of a mess. It is a work in progress, that just tests
 * the ZLayer creation from explicit Pravega client configuration.
 */
object PravegaLayers extends SharedPravegaContainerSpec("PravegaLayers") {

  import CommonTestSettings._

  private def keyValueTestStream(a: Int, b: Int): ZStream[Any, Nothing, (String, Int)] = ZStream
    .fromIterable(a until b)
    .map(i => (f"$i%04d", i))

  override def spec: Spec[Environment with TestEnvironment with Scope, Any] = suite("PravegaLayers")(
    createScopes,
    createStreams,
    createGroups,
    createTables,
    runTests,
    dropGroups,
    dropTables,
    dropStreams,
    dropScope
  ).provide(
    Scope.default,
    PravegaReaderGroupManager.live(aScope, clientConfig),
    PravegaStreamManager.live(clientConfig),
    PravegaTableManager.live(clientConfig),
    PravegaStream.fromScope(aScope, clientConfig),
    PravegaTable.fromScope(aScope, clientConfig)
  ) @@ sequential

  private def createScopes = suite("Scopes")(
    test("Scope created once")(PravegaStreamManager.createScope(aScope).map(once => assert(once)(isTrue))),
    test("Scope skip twice")(PravegaStreamManager.createScope(aScope).map(twice => assert(twice)(isFalse)))
  ) @@ sequential

  private def createStreams = suite("Streams")(
    test("Stream created once")(
      PravegaStreamManager.createStream(aScope, "stream", staticStreamConfig(2)).map(once => assert(once)(isTrue))
    ),
    test("Stream skip twice")(
      PravegaStreamManager.createStream(aScope, "stream", staticStreamConfig(2)).map(twice => assert(twice)(isFalse))
    )
  ) @@ sequential

  private def dropStreams = suite("Drop streams")(
    test("Stream dropped once")(
      PravegaStreamManager.sealStream(aScope, "stream") *> PravegaStreamManager
        .deleteStream(aScope, "stream")
        .map(once => assert(once)(isTrue))
    ),
    test("Stream skip twice")(PravegaStreamManager.deleteStream(aScope, "stream").map(twice => assert(twice)(isFalse)))
  ) @@ sequential

  private def createGroups = suite("Groups")(
    test("Group created")(
      PravegaReaderGroupManager.createReaderGroup("group", "stream").map(once => assert(once)(isTrue))
    ),
    test("Group created")(
      PravegaReaderGroupManager
        .createReaderGroup(
          "group2",
          ReaderGroupConfig.builder.automaticCheckpointIntervalMillis(1000),
          "stream"
        )
        .map(once => assert(once)(isTrue))
    ),
    test("Open")(PravegaReaderGroupManager.openReaderGroup("group").map(_ => assertCompletes))
  )

  private def runTests = suite("Run tests")(
    test("Run tests")(
      for {
        _ <- testStream(1, 2) >>> sink("stream")
        _ <- keyValueTestStream(1, 2) >>> PravegaTable.sink("table", tableWriterSettings, (a: Int, b: Int) => a + b)
      } yield (assertCompletes)
    )
  )

  private def dropGroups = suite("Drop groups")(
    test("Group dropped once")(
      PravegaReaderGroupManager.dropReaderGroup(aScope, "group").map(once => assert(once)(isTrue))
    ),
    test("Group skip twice")(
      PravegaReaderGroupManager.dropReaderGroup(aScope, "group2").map(twice => assert(twice)(isTrue))
    )
  )

  private def createTables = {
    val tableConfig = KeyValueTableConfiguration.builder().partitionCount(2).primaryKeyLength(4).build()
    suite("Tables")(
      test("Table created once")(
        PravegaTableManager.createTable(aScope, "table", tableConfig).map(once => assert(once)(isTrue))
      ),
      test("Table skip twice")(
        PravegaTableManager.createTable(aScope, "table", tableConfig).map(twice => assert(twice)(isFalse))
      )
    ) @@ sequential
  }
  private def dropTables =
    test("Drop table")(PravegaTableManager.dropTable(aScope, "table").map(dropped => assert(dropped)(isTrue)))
  private def dropScope =
    test("Drop namespace")(PravegaStreamManager.deleteScope(aScope).map(dropped => assert(dropped)(isTrue)))
}
