---
layout: main
title: ZIO Pravega
---

# ZIO Pravega.

![Pravega](/images/pravega-bird-lrg.png)


ZIO Pravega is a Scala library for [Pravega](https://pravega.io/) based on [ZIO](https://zio.dev/).

It provides a high-level API for reading and writing to Pravega streams and Pravega Key Value Pair tables.

## Modules

ZIO Pravega is split into the following modules:

* `zio-pravega` - Core module for reading and writing to Pravega streams and Pravega Key Value Pair tables.
* `zio-pravega-saga` - Module for using ZIO Pravega in a [Saga](https://microservices.io/patterns/data/saga.html) pattern.


## Installation

```scala
libraryDependencies += "@ORG@" %% "zio-pravega" % "@VERSION@"
libraryDependencies += "@ORG@" %% "zio-pravega-saga" % "@VERSION@"
```


