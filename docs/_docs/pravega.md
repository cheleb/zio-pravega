---
layout: main
---

# Pravega.

## What is Pravega?

Pravega is an open source distributed storage service implementing Streams. It offers Stream as the main primitive for the foundation of reliable storage systems.

A Stream is an unbounded sequence of bytes (events) that is written to and read from in a strictly sequential order. Streams are durable and elastic, making them ideal for storing events and data that must be reliably and durably ingested and processed in real-time.

Out of the box, Pravega provides:
* strong consistency guarantees for both reads and writes.
* exactly-once semantics for writes.
* transactions.
* stream retention policies:
    * time-based retention.
    * size-based retention.
    * truncation.
    * long-term retention (tiered storage).
* automatic stream scaling policies:
    * Up and **down** scaling.
    * fixed number of segments.
    * rate of events per second per segment.
    * rate of bytes per second per segment.
* stream metadata management.




## Run Pravega cluster

### For local developpement 

Just download the [Pravega distribution](https://github.com/pravega/pravega/releases) and run the following command.

```bash
./bin/pravega-standalone
```
### Run Pravega cluster on Docker

ZIO Pravega provides a docker-compose file to run a local Pravega cluster.

```bash
docker-compose -f docker-compose.yml up
```

## Run Pravega cluster on Kubernetes

TBD

### Description
