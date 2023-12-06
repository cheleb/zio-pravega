package zio.pravega

import zio._
import zio.test._
import zio.test.Assertion._
import zio.test.TestEnvironment
import io.pravega.client.EventStreamClientFactory
import java.util.UUID
import io.pravega.client.stream.impl.UTF8StringSerializer
import io.pravega.client.stream.ReaderConfig
import zio.pravega.admin.PravegaReaderGroupManager

object ReaderOfflineSpec extends SharedPravegaContainerSpec("reader-offline") {

  override def spec: Spec[Environment with TestEnvironment, Any] = scopedSuite(test("Set reader offline")(for {
    _ <- createStream("stream", 2)
    _ <- createGroup("unclosed", "stream")
    _ <- ZIO.debug("""
        🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥
        🔥   Pravega container noisy log expected below  🔥
        🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥""")
    n <- ZIO.scoped(
           ZIO
             .attemptBlocking(EventStreamClientFactory.withScope(aScope, clientConfig))
             .withFinalizerAuto
             .map(
               _.createReader( // This reader is intentionaly not close to leak
                 UUID.randomUUID().toString,
                 "unclosed",
                 new UTF8StringSerializer,
                 ReaderConfig.builder().build()
               )
             ) *> PravegaReaderGroupManager.readerOffline("unclosed")
         )
  } yield assert(n)(equalTo(1))))

}
