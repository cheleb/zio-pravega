# Pravega ZIO

![scala](https://github.com/cheleb/zio-pravega/actions/workflows/ci.yml/badge.svg)
[![codecov](https://codecov.io/gh/cheleb/zio-pravega/branch/master/graph/badge.svg?token=9IW44171RJ)](https://codecov.io/gh/cheleb/zio-pravega)

This project is [ZIO](https://www.zio.dev) connector to [Pravega](https://www.pravega.io)

* Streaming API
* Key Value Pair table API


More information at [documentation](https://cheleb.github.io/zio-pravega/docs/zio-pravega/index.html)

# Contribute

You need to have a Pravega instance running locally. You can use the [Pravega docker image](https://hub.docker.com/r/pravega/pravega).

TestContainer is used to run the tests. You can change pravega image with the following environment variable:

```bash
export PRAVEGA_IMAGE=cheleb/pravega:0.13.0
```

This image is based on the official Pravega image but with a custom configuration to run on a single machine and M1 architecture.