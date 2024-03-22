# Getting Started

## Installation in existing project.

```sbt
libraryDependencies += "dev.cheleb" %% "zio-pravega" % "@VERSION@"
```


## Create a ZIO Pravega project

ZIO Pravega provides very simple giter8 template to create a ZIO Pravega project.

```shell
sbt new cheleb/zio-pravega-starter.g8
```

cd into the project directory and run the following command to start the application.

This stater illusrate how to connect, to a Pravega cluster and:

* Create the resources.
* Create the pipeline for writing.
* Create the pipeline for processing.
