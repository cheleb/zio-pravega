---
sidebar_position: 1
---
# ZIO Pravega

This project is a [ZIO](https://zio.dev) 2.x connector to [Pravega](https://pravega.io).

# Quickstart 

Assumming [Pravega](https://pravega.io) is running on tcp://localhost:9090 see https://github.com/pravega/pravega/releases
## Init project
```bash
sbt new cheleb/zio-pravega-starter.g8
```

## Open sbt session

```bash
cd my-zio-pravega-project
sbt
```

## Enjoy

Then in sbt session:

Create scope and stream:

```sbtshell
runMain dev.sample.myziopravegaproject.CreateResourcesExample
```

Write in stream:

```sbtshell
runMain dev.sample.myziopravegaproject.StreamWriteExample
```

Read from Stream:

```sbtshell
runMain dev.sample.myziopravegaproject.StreamReadExample
```


# Installation

To install the dependency

```scala
libraryDependencies += "@ORG@" %% "zio-pravega" % "0.0.1"
```

