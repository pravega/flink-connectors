<!--
Copyright Pravega Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# Pravega Flink Connectors [![Build Status](https://travis-ci.org/pravega/flink-connectors.svg?branch=master)](https://travis-ci.org/pravega/flink-connectors)

This repository implements connectors to read and write [Pravega](http://pravega.io/) Streams with [Apache Flink](http://flink.apache.org/) stream processing framework.

The connectors can be used to build end-to-end stream processing pipelines (see [Samples](https://github.com/pravega/pravega-samples)) that use Pravega as the stream storage and message bus, and Apache Flink for computation over the streams.


## Features & Highlights

  - **Exactly-once processing guarantees** for both Reader and Writer, supporting **end-to-end exactly-once processing pipelines**

  - Seamless integration with Flink's checkpoints and savepoints.

  - Parallel Readers and Writers supporting high throughput and low latency processing.

  - Table API support to access Pravega Streams for both **Batch** and **Streaming** use case.

## Compatibility Matrix
The [master](https://github.com/pravega/flink-connectors) branch will always have the most recent
supported versions of Flink and Pravega.

| Git Branch | Pravega Version | Java Version To Build Connector | Java Version To Run Connector | Flink Version | Status | Artifact Link |
|-------------------------------------------------------------------------------------|------|---------|--------------|------|-------------------|----------------------------------------------------------------------------------------|
| [master](https://github.com/pravega/flink-connectors)                               | 0.10 | Java 11 | Java 8 or 11 | 1.13 | Under Development | https://github.com/pravega/flink-connectors/packages/910737 |
| [r0.10-flink1.12](https://github.com/pravega/flink-connectors/tree/r0.10-flink1.12) | 0.10 | Java 11 | Java 8 or 11 | 1.12 | Under Development | https://github.com/pravega/flink-connectors/packages/887087 |
| [r0.10-flink1.11](https://github.com/pravega/flink-connectors/tree/r0.10-flink1.11) | 0.10 | Java 11 | Java 8 or 11 | 1.11 | Under Development | https://github.com/pravega/flink-connectors/packages/904171 |
| [r0.9](https://github.com/pravega/flink-connectors/tree/r0.9)                       | 0.9  | Java 11 | Java 8 or 11 | 1.11 | Released          | https://repo1.maven.org/maven2/io/pravega/pravega-connectors-flink-1.11_2.12/0.9.1/    |
| [r0.9-flink1.10](https://github.com/pravega/flink-connectors/tree/r0.9-flink1.10)   | 0.9  | Java 11 | Java 8 or 11 | 1.10 | Released          | https://repo1.maven.org/maven2/io/pravega/pravega-connectors-flink-1.10_2.12/0.9.1/    |
| [r0.9-flink1.9](https://github.com/pravega/flink-connectors/tree/r0.9-flink1.9)     | 0.9  | Java 11 | Java 8 or 11 | 1.9  | Released          | https://repo1.maven.org/maven2/io/pravega/pravega-connectors-flink-1.9_2.12/0.9.1/     |

## How to build
Building the connectors from the source is only necessary when we want to use or contribute to the latest (unreleased) version of the Pravega Flink connectors.

The connector project is linked to a specific version of Pravega, based on the `pravegaVersion` field in `gradle.properties`. 

Checkout the source code repository by following below steps:

```git clone https://github.com/pravega/flink-connectors.git```

After cloning the repository, the project can be built (excluding tests) by running the below command in the project root directory flink-connectors.

```./gradlew clean build -x test```

## How to use

Check out documents [here](https://github.com/pravega/flink-connectors/blob/master/documentation/src/docs/dev-guide.md) to learn how to build your own applications using Flink connector for Pravega.

More examples on how to use the connectors with Flink application can be found in [Pravega Samples](https://github.com/pravega/pravega-samples/tree/master/flink-connector-examples) repository.

## Support

Don't hesitate to ask! Contact the developers and community on [Slack](https://pravega-io.slack.com/) ([signup](https://pravega-slack-invite.herokuapp.com/)) if you need any help. Open an issue if you found a bug on [Github Issues](https://github.com/pravega/flink-connectors/issues).

## About

Flink connectors for Pravega is 100% open source and community-driven. All components are available
under [Apache 2 License](https://www.apache.org/licenses/LICENSE-2.0.html) on GitHub.
