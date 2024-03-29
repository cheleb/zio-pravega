package zio.pravega

import zio._
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._

import io.pravega.client.tables.KeyValueTableConfiguration
import io.pravega.client.stream.ReaderGroupConfig
import zio.pravega.admin._
import zio.stream.ZSink

object AdminSpec extends SharedPravegaContainerSpec("admin") {

  override def spec: Spec[Environment & TestEnvironment & Scope, Any] = suite("Admin")(
    createScopes,
    listScopes,
    createStreams,
    listStreams,
    createGroups,
    createTables,
    listTables,
    dropGroups,
    dropTables,
    dropStreams,
    dropScope
  ).provide(
    Scope.default,
    PravegaClientConfig.live,
    PravegaReaderGroupManager.live(aScope),
    PravegaStreamManager.live,
    PravegaTableManager.live
  ) @@ sequential

  private def createScopes = suite("Scopes")(
    test("Scope created once")(PravegaStreamManager.createScope(aScope).map(once => assert(once)(isTrue))),
    test("Scope skip twice")(PravegaStreamManager.createScope(aScope).map(twice => assert(twice)(isFalse)))
  ) @@ sequential

  private def listScopes =
    test("List scopes")(
      PravegaStreamManager.listScopes.run(ZSink.collectAll).map(scopes => assert(scopes)(contains(aScope)))
    )

  private def createStreams = suite("Streams")(
    test("Stream created once")(
      PravegaStreamManager.createStream(aScope, "stream", staticStreamConfig(2)).map(once => assert(once)(isTrue))
    ),
    test("Stream skip twice")(
      PravegaStreamManager.createStream(aScope, "stream", staticStreamConfig(2)).map(twice => assert(twice)(isFalse))
    )
  ) @@ sequential

  private def listStreams =
    test("List streams")(
      PravegaStreamManager
        .listStreams(aScope)
        .run(ZSink.collectAll)
        .map(streams => assert(streams.map(_.getStreamName))(contains("stream")))
    )

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

  private def listTables =
    test("List tables")(
      PravegaTableManager
        .listTables(aScope)
        .run(ZSink.collectAll)
        .map(tables => assert(tables.map(_.getKeyValueTableName))(contains("table")))
    )

  private def dropTables =
    test("Drop table")(PravegaTableManager.deleteTable(aScope, "table").map(dropped => assert(dropped)(isTrue)))
  private def dropScope =
    test("Drop namespace")(PravegaStreamManager.deleteScope(aScope).map(dropped => assert(dropped)(isTrue)))
}
