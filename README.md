# Pravega ZIO

![scala](https://github.com/cheleb/zio-pravega/actions/workflows/scala.yml/badge.svg)
[![codecov](https://codecov.io/gh/cheleb/zio-pravega/branch/master/graph/badge.svg?token=9IW44171RJ)](https://codecov.io/gh/cheleb/zio-pravega)

[ZIO](https://www.zio.dev) connector to [Pravega](https://www.pravega.io)

See [documentation](https://cheleb.github.io/zio-pravega/)

# Contribute

You need to have a Pravega instance running locally. You can use the [Pravega docker image](https://hub.docker.com/r/pravega/pravega).

TestContainer is used to run the tests. You can change pravega image with the following environment variable:

```bash
export PRAVEGA_IMAGE=cheleb/pravega-m1:0.10.1
```

This image is based on the official Pravega image but with a custom configuration to run on a single machine and M1 architecture.