<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Pravega Flink Connectors [![Build Status](https://travis-ci.org/pravega/flink-connectors.svg?branch=master)](https://travis-ci.org/pravega/flink-connectors)

Connectors to read and write [Pravega](http://pravega.io/) Streams with [Apache Flink](http://flink.apache.org/) stream processing framework.

Build end-to-end stream processing pipelines that use Pravega as the stream storage and message bus, and Apache Flink for computation over the streams.


## Features & Highlights

  - **Exactly-once processing guarantees** for both Reader and Writer, supporting **end-to-end exactly-once processing pipelines**

  - Seamless integration with Flink's checkpoints and savepoints.

  - Parallel Readers and Writers supporting high throughput and low latency processing.

  - Table API support to access Pravega Streams for both **Batch** and **Streaming** use case.

## Building Connectors

Building the connectors from the source is only necessary when we want to use or contribute to the latest (*unreleased*) version of the Pravega Flink connectors.

The connector project is linked to a specific version of Pravega, based on a [git submodule](https://git-scm.com/book/en/v2/Git-Tools-Submodules) pointing to a commit-id. By default the sub-module option is disabled and the build step will make use of the Pravega version defined in the `gradle.properties` file. You could override this option by enabling `usePravegaVersionSubModule` flag in `gradle.properties` to `true`.

Checkout the source code repository by following below steps:

```
git clone --recursive https://github.com/pravega/flink-connectors.git
```

After cloning the repository, the project can be built by running the below command in the project root directory `flink-connectors`.

```
./gradlew clean build
```

To install the artifacts in the local maven repository cache `~/.m2/repository`, run the following command:

```
./gradlew clean install
```

### Customizing the Build

#### Building against a custom Flink version

We can check and change the Flink version that Pravega builds against via the `flinkVersion` variable in the `gradle.properties` file.

**Note**: Only Flink versions that are compatible with the latest connector code can be chosen.

#### Building against another Scala version

This section is only relevant if you use [Scala](https://www.scala-lang.org/) in the stream processing application in with Flink and Pravega.

Parts of the Apache Flink use the language or depend on libraries written in Scala. Because Scala is **not** strictly compatible across versions, there exist different versions of Flink compiled for different Scala versions.
If we use Scala code in the same application where we use the Apache Flink or the Flink connectors, we typically have to make sure we use a version of Flink that uses the same Scala version as our application.

By default, the dependencies point to Flink for Scala **2.11**.
To depend on released Flink artifacts for a different Scala version, you need to edit the `build.gradle` file and change all entries for the Flink dependencies to have a different Scala version suffix. For example, `flink-streaming-java_2.11` would be replaced by `flink-streaming-java_2.12` for Scala **2.12**.

In order to build a new version of Flink for a different Scala version, please refer to the [Flink documentation](https://ci.apache.org/projects/flink/flink-docs-stable/start/building.html#scala-versions).

## Setting up your IDE

Connector project uses [Project Lombok](https://projectlombok.org/), so we should ensure that we have our IDE setup with the required plugins. (**IntelliJ is recommended**).

To import the source into IntelliJ:

1. Import the project directory into IntelliJ IDE. It will automatically detect the gradle project and import things correctly.
2. Enable `Annotation Processing` by going to `Build, Execution, Deployment` -> `Compiler` > `Annotation Processors` and checking `Enable annotation processing`.
3. Install the `Lombok Plugin`. This can be found in `Preferences` -> `Plugins`. Restart your IDE.
4. Connectors project compiles properly after applying the above steps.

For eclipse, we can generate eclipse project files by running `./gradlew eclipse`.

## Releases

The latest releases can be found on the [Github Release](https://github.com/pravega/flink-connectors/releases) project page.

## Support

Don’t hesitate to ask! Contact the developers and community on the  [Slack](https://pravega-io.slack.com/) if you need any help.
Open an issue if you found a bug on [Github
Issues](https://github.com/pravega/flink-connectors/issues).

## Documentation
See the [Project Wiki](https://github.com/pravega/flink-connectors/wiki) for documentation on how to build and use the Flink Connector library.

More examples on how to use the connectors with Flink application can be found in [Pravega Samples](https://github.com/pravega/pravega-samples) repository.

## About

Flink connectors for Pravega is 100% open source and community-driven. All components are available
under [Apache 2 License](https://www.apache.org/licenses/LICENSE-2.0.html) on
GitHub.
