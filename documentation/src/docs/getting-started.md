<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Pravega Flink Connectors [![Build Status](https://travis-ci.org/pravega/flink-connectors.svg?branch=master)](https://travis-ci.org/pravega/flink-connectors)

This repository implements connectors to read and write [Pravega](http://pravega.io/) Streams with [Apache Flink](http://flink.apache.org/) stream processing framework.

The connectors can be used to build end-to-end stream processing pipelines (see [Samples](https://github.com/pravega/pravega-samples)) that use Pravega as the stream storage and message bus, and Apache Flink for computation over the streams.


## Features & Highlights

  - **Exactly-once processing guarantees** for both Reader and Writer, supporting **end-to-end exactly-once processing pipelines**

  - Seamless integration with Flink's checkpoints and savepoints.

  - Parallel Readers and Writers supporting high throughput and low latency processing.

  - Table API support to access Pravega Streams for both **Batch** and **Streaming** use case.

## Building Connectors

Building the connectors from the source is only necessary when we want to use or contribute to the latest (*unreleased*) version of the Pravega Flink connectors.

The connector project is linked to a specific version of Pravega and the version is defined in the `gradle.properties` file (`pravegaVersion`).

Checkout the source code repository by following below steps:

```
git clone https://github.com/pravega/flink-connectors.git
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
Open an issue if you found a bug on [Github Issues](https://github.com/pravega/flink-connectors/issues).

## Samples

Follow the [Pravega Samples](https://github.com/pravega/pravega-samples) repository to learn more about how to build and use the Flink Connector library.