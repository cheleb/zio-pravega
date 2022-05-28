---
sidebar_position: 3
---

# Client configuration


Pravega client configuration has many options. Default values are found in [reference.conf](https://github.com/cheleb/zio-pravega/blob/master/src/main/resources/reference.conf) and can be overriden globaly in application.conf or locally in a programatic way.

```scala mdoc:invisible
import io.pravega.client.ClientConfig
import java.net.URI
```



```scala mdoc

val clientConfig = ClientConfig
    .builder()
    .controllerURI(new URI("tcp://localhost:9090:"))
    .enableTlsToController(true)
    .build()
```