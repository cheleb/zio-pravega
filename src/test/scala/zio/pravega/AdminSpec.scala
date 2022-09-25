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
      createScopes,
      createStreams,
      createGroups,
      createTables,
      dropGroups,
      dropTables,
      dropStreams,
      dropScope
    ).provide(Scope.default, PravegaAdmin.live(clientConfig)) @@ sequential

  def createScopes = suite("Scopes")(
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

  def createStreams =
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

  def dropStreams =
    suite("Drop streams")(
      test("Stream dropped once")(
        PravegaAdmin
          .sealStream(aScope, "stream")
          *>
            PravegaAdmin
              .dropStream(
                aScope,
                "stream"
              )
              .map(once => assert(once)(isTrue))
      ),
      test("Stream skip twice")(
        PravegaAdmin
          .dropStream(
            aScope,
            "stream"
          )
          .map(twice => assert(twice)(isFalse))
      )
    ) @@ sequential

  def createGroups = suite("Groups")(
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

  def dropGroups = suite("Drop groups")(
    test("Group dropped once")(
      PravegaAdmin
        .dropReaderGroup(
          aScope,
          "group"
        )
        .map(once => assert(once)(isTrue))
    ),
    test("Group skip twice")(
      PravegaAdmin
        .dropReaderGroup(
          aScope,
          "group2"
        )
        .map(twice => assert(twice)(isTrue))
    )
  )

  def createTables = {
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
  def dropTables =
    test("Drop table")(
      PravegaAdmin
        .dropTable(
          aScope,
          "table"
        )
        .map(dropped => assert(dropped)(isTrue))
    )
  def dropScope =
    test("Drop namespace")(
      PravegaAdmin
        .dropScope(aScope)
        .map(dropped => assert(dropped)(isTrue))
    )
}
