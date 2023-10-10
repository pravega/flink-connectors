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

# Pravega Flink Connectors

[![Build Status](https://github.com/pravega/flink-connectors/actions/workflows/build.yml/badge.svg)](https://github.com/pravega/flink-connectors/actions/workflows/build.yml?query=branch%3Amaster) [![License](https://img.shields.io/github/license/pravega/flink-connectors)](https://github.com/pravega/flink-connectors/blob/master/LICENSE) [![Downloads](https://img.shields.io/github/downloads/pravega/flink-connectors/total)](https://github.com/pravega/flink-connectors/releases) [![Codecov](https://img.shields.io/codecov/c/github/pravega/flink-connectors)](https://app.codecov.io/gh/pravega/flink-connectors/)

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

| Git Branch                                                                          | Pravega Version | Flink Version | Status            | Artifact Link                                                                        |
|-------------------------------------------------------------------------------------|-----------------|---------------|-------------------|--------------------------------------------------------------------------------------|
| [master](https://github.com/pravega/flink-connectors)                               | 0.14            | 1.16          | Under Development | https://github.com/pravega/flink-connectors/packages/1441637                         |
| [r0.13](https://github.com/pravega/flink-connectors/tree/r0.13)                     | 0.13            | 1.16          | Released          | https://repo1.maven.org/maven2/io/pravega/pravega-connectors-flink-1.16_2.12/0.13.0/ |
| [r0.13-flink1.15](https://github.com/pravega/flink-connectors/tree/r0.13-flink1.15) | 0.13            | 1.15          | Released          | https://repo1.maven.org/maven2/io/pravega/pravega-connectors-flink-1.15_2.12/0.13.0/ |
| [r0.13-flink1.14](https://github.com/pravega/flink-connectors/tree/r0.13-flink1.14) | 0.13            | 1.14          | Released          | https://repo1.maven.org/maven2/io/pravega/pravega-connectors-flink-1.14_2.12/0.13.0/ |
| [r0.12](https://github.com/pravega/flink-connectors/tree/r0.12)                     | 0.12            | 1.15          | Released          | https://repo1.maven.org/maven2/io/pravega/pravega-connectors-flink-1.15_2.12/0.12.0/ |
| [r0.12-flink1.14](https://github.com/pravega/flink-connectors/tree/r0.12-flink1.14) | 0.12            | 1.14          | Released          | https://repo1.maven.org/maven2/io/pravega/pravega-connectors-flink-1.14_2.12/0.12.0/ |
| [r0.12-flink1.13](https://github.com/pravega/flink-connectors/tree/r0.12-flink1.13) | 0.12            | 1.13          | Released          | https://repo1.maven.org/maven2/io/pravega/pravega-connectors-flink-1.13_2.12/0.12.0/ |

## How to build

Building the connectors from the source is only necessary when we want to use or contribute to the latest (unreleased) version of the Pravega Flink connectors.

To build the project, Java version 11 is required and the repository needs to be checkout via `git clone https://github.com/pravega/flink-connectors.git`.

> The connector project is linked to a specific version of Pravega, based on the `pravegaVersion` field in the `gradle.properties`.

After cloning the repository, the project can be built (excluding tests) by running the below command in the project root directory flink-connectors.

```./gradlew clean build -x test```

## How to use

Check out documents [here](https://github.com/pravega/flink-connectors/blob/master/documentation/src/docs/dev-guide.md) to learn how to build your own applications using Flink connector for Pravega. Also watch out that the Java version required to run the connector is either 8 or 11.

More examples on how to use the connectors with Flink application can be found in [Pravega Samples](https://github.com/pravega/pravega-samples/tree/master/flink-connector-examples) repository.

## Support

Don't hesitate to ask! Contact the developers and community on [Slack](https://pravega-io.slack.com/) ([signup](https://pravega-slack-invite.herokuapp.com/)) if you need any help. Open an issue if you found a bug on [Github Issues](https://github.com/pravega/flink-connectors/issues).

## About

Flink connectors for Pravega is 100% open source and community-driven. All components are available
under [Apache 2 License](https://www.apache.org/licenses/LICENSE-2.0.html) on GitHub.
