package zio.pravega

import zio._
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._
import io.pravega.client.stream.StreamConfiguration
import io.pravega.client.stream.ScalingPolicy
import io.pravega.client.tables.KeyValueTableConfiguration
import io.pravega.client.EventStreamClientFactory
import io.pravega.client.ClientConfig
import java.util.UUID
import io.pravega.client.stream.impl.UTF8StringSerializer
import io.pravega.client.stream.ReaderConfig

trait AdminSpec {
  this: ZIOSpec[
    PravegaStreamService & PravegaAdminService & PravegaTableService
  ] =>

  private val tableConfig = KeyValueTableConfiguration
    .builder()
    .partitionCount(2)
    .primaryKeyLength(4)
    .build()

  def adminSuite(
      pravegaScope: String,
      pravegaStreamName: String,
      groupName: String
  ): Spec[PravegaAdminService, Throwable] =
    suite("Admin")(
      test("Scope created once")(
        ZIO
          .scoped(PravegaAdminService.createScope(pravegaScope))
          .map(once => assert(once)(isTrue))
      ),
      test("Scope skip twice")(
        ZIO
          .scoped(PravegaAdminService.createScope(pravegaScope))
          .map(twice => assert(twice)(isFalse))
      ),
      test("Stream created once")(
        ZIO
          .scoped(
            PravegaAdminService.createStream(
              pravegaStreamName,
              StreamConfiguration.builder
                .scalingPolicy(ScalingPolicy.fixed(8))
                .build,
              pravegaScope
            )
          )
          .map(once => assert(once)(isTrue))
      ),
      test("Stream creation skiped")(
        ZIO
          .scoped(
            PravegaAdminService.createStream(
              pravegaStreamName,
              StreamConfiguration.builder
                .scalingPolicy(ScalingPolicy.fixed(8))
                .build,
              pravegaScope
            )
          )
          .map(twice => assert(twice)(isFalse))
      ),
      test(s"Create group $groupName")(
        ZIO
          .scoped(
            PravegaAdminService.readerGroup(
              pravegaScope,
              groupName,
              pravegaStreamName
            )
          )
          .map(once => assert(once)(isTrue))
      ),
      test("Create reader buggy")(
        ZIO
          .scoped(
            PravegaAdminService.readerGroup(
              "zio-scope",
              "coco-buggy",
              pravegaStreamName
            ) *> PravegaAdminService.readerGroup(
              "zio-scope",
              "coco-buggy2",
              pravegaStreamName
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
            PravegaAdminService.createTable(
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
                  _.createReader(
                    UUID.randomUUID().toString,
                    "coco-buggy",
                    new UTF8StringSerializer,
                    ReaderConfig.builder().build()
                  )
                ) *> PravegaAdminService
                .readerOffline("zio-scope", "coco-buggy")
            )
            .map(n => assert(n)(equalTo(1)))
      )
    ) @@ sequential

}
