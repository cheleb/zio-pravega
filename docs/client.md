---
sidebar_position: 3
---

# Client configuration


Pravega client configuration has many options. Default values are found in [reference.conf](https://github.com/cheleb/zio-pravega/blob/master/src/main/resources/reference.conf) and can be overriden globaly in application.conf or locally in a programatic way.

```scala mdoc:silent
import zio.pravega._
import java.net.URI

val clientConfig =  PravegaClientConfig.builder
    .controllerURI(new URI("tcp://localhost:9090:"))
    .enableTlsToController(true)
    .build()
```