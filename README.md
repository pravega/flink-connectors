<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Pravega Flink Connectors [![Build Status](https://travis-ci.org/pravega/flink-connectors.svg?branch=master)](https://travis-ci.org/pravega/flink-connectors)

### Project Status

## Table of Contents
- [Overview](#overview)
- [Requirements](#requirements)
- [Quick Start](#quick-start)
    - [Creating a Flink stream processing project](https://github.com/pravega/flink-connectors/wiki/Project-Setup#creating-a-flink-stream-processing-project)
    - [Add the connector dependencies](https://github.com/pravega/flink-connectors/wiki/Project-Setup#add-the-connector-dependencies)
    - [Running / deploying the application](
https://github.com/pravega/flink-connectors/wiki/Project-Setup#running--deploying-the-application)
- [Usage](https://github.com/pravega/flink-connectors/wiki/Configuration)
  - [Configurations](https://github.com/pravega/flink-connectors/wiki/Configuration)
      - [PravegaConfig Class](https://github.com/pravega/flink-connectors/wiki/Configuration#pravegaconfig-class)
      - [Creating PravegaConfig](https://github.com/pravega/flink-connectors/wiki/Configuration#creating-pravegaconfig)
      - [Using PravegaConfig](https://github.com/pravega/flink-connectors/wiki/Configuration#using-pravegaconfig)
      - [Configuration Elements](https://github.com/pravega/flink-connectors/wiki/Configuration#configuration-elements)
      - [Understanding the Default Scope](https://github.com/pravega/flink-connectors/wiki/Configuration#understanding-the-default-scope)
  - [Streaming Connectors](https://github.com/pravega/flink-connectors/wiki/Streaming-Connector)
      - [Data Source (Reader)](https://github.com/pravega/flink-connectors/wiki/Streaming-Connector#data-source-reader)
          - [Parameters](https://github.com/pravega/flink-connectors/wiki/Streaming-Connector#parameters)
          - [Input Stream(s)](https://github.com/pravega/flink-connectors/wiki/Streaming-Connector#input-streams)
          - [Parallelism](https://github.com/pravega/flink-connectors/wiki/Streaming-Connector#parallelism)
          - [Checkpointing](https://github.com/pravega/flink-connectors/wiki/Streaming-Connector#checkpointing)
          - [Timestamp Extraction / Watermark Emission](https://github.com/pravega/flink-connectors/wiki/Streaming-Connector#timestamp-extraction--watermark-emission)
          - [Historical Stream Processing](https://github.com/pravega/flink-connectors/wiki/Streaming-Connector#historical-stream-processing)
     - [Data Sink (Writer)](https://github.com/pravega/flink-connectors/wiki/Streaming-Connector#data-sink-writer)
          - [Parameters](https://github.com/pravega/flink-connectors/wiki/Streaming-Connector#parameters-1)
          - [Parallelism](https://github.com/pravega/flink-connectors/wiki/Streaming-Connector#parallelism-1)
          - [Event Routing](https://github.com/pravega/flink-connectors/wiki/Streaming-Connector#event-routing)
          - [Event Time Ordering](https://github.com/pravega/flink-connectors/wiki/Streaming-Connector#event-time-ordering)
          - [Writer Modes](https://github.com/pravega/flink-connectors/wiki/Streaming-Connector#writer-modes)
      - [Batch Connectors](https://github.com/pravega/flink-connectors/wiki/Batch-Connector)
          - [Data Source (InputFormat)](https://github.com/pravega/flink-connectors/wiki/Batch-Connector#data-source-inputformat)
             - [Parameters](https://github.com/pravega/flink-connectors/wiki/Batch-Connector#parameters)
             - [Input Stream(s)](https://github.com/pravega/flink-connectors/wiki/Batch-Connector#input-streams)
             - [Stream Cuts](https://github.com/pravega/flink-connectors/wiki/Batch-Connector#stream-cuts)
             - [Parallelism](https://github.com/pravega/flink-connectors/wiki/Batch-Connector#parallelism)
          - Data Source (OutputFormat)
  - [Table Connectors](https://github.com/pravega/flink-connectors/wiki/Table-Connector)
      - [Table Source](https://github.com/pravega/flink-connectors/wiki/Table-Connector#table-source)
          - [Parameters](https://github.com/pravega/flink-connectors/wiki/Table-Connector#parameters)
          - [Custom Formats](https://github.com/pravega/flink-connectors/wiki/Table-Connector#custom-formats)
      - [Table Sink](https://github.com/pravega/flink-connectors/wiki/Table-Connector#table-sink)
          - [Parameters](https://github.com/pravega/flink-connectors/wiki/Table-Connector#parameters-1)
          - [Custom Formats](https://github.com/pravega/flink-connectors/wiki/Table-Connector#custom-formats-1)
- [Data Serialization](https://github.com/pravega/flink-connectors/wiki/Data-Serialization)
- [Development](https://github.com/pravega/flink-connectors/wiki/Building)
   - [Build The Connector](https://github.com/pravega/flink-connectors/wiki/Building)
       - [Clone the flink-connectors Repository](https://github.com/pravega/flink-connectors/wiki/Building#clone-the-flink-connectors-repository)
       - [Build and install the Flink Connector library](https://github.com/pravega/flink-connectors/wiki/Building#build-and-install-the-flink-connector-library)
   - [Customizing The Build](https://github.com/pravega/flink-connectors/wiki/Building#customizing-the-build)
      - [Building against a custom Flink version](https://github.com/pravega/flink-connectors/wiki/Building#building-against-a-custom-flink-version)
      - [Building against another Scala version](https://github.com/pravega/flink-connectors/wiki/Building#building-against-another-scala-version)
      - [Setting up your IDE](#setting-up-your-ide)
      - [Updating the Pravega dependency](https://github.com/pravega/flink-connectors/wiki/Building#updating-the-pravega-dependency)
- Metrics
- Release Management
- Releases
- Contributing


## Overview

Connectors are used to [Read and Write](http://pravega.io/docs/latest/basic-reader-and-writer/#working-with-pravega-basic-reader-and-writer) [Pravega](http://pravega.io/docs/v0.3.2/pravega-concepts/) streams with [Apache Flink](http://flink.apache.org/) stream processing applications.

We can build end-to-end stream processing pipelines using Pravega as the stream storage and message bus. It uses [Apache Flink](https://flink.apache.org/) for performing computations over the streams.  


### Key Features

  - **Exactly-Once Semantics:** Pravega ensures that each event is delivered and processed exactly once, with exact ordering guarantees, despite failures in clients, servers or the network. Thus, offering **[Exactly-once processing guarantees]**(http://pravega.io/docs/v0.3.2/key-features/#exactly-once-semantics)for both reader and writer, supporting **end-to-end exactly-once processing pipelines**.

  - Possess seamless integration with [Flink's checkpoints and savepoints](https://github.com/pravega/flink-connectors/wiki/Streaming-Connector#checkpointing).

  - Allows parallel readers and writers by supporting high throughput and low latency processing.

## Requirements

  - **Java 8** or greater is required.
  - [Apache Flink](https://flink.apache.org/downloads.html#binaries)version 1.4 or newer.
# Quick Start

## Setting up your IDE

 Pravega uses [Project Lombok](https://projectlombok.org/) so you should ensure you have your IDE setup with the required plugins. Using IntelliJ is recommended.

 To import the source into IntelliJ:

 1. Import the project directory into IntelliJ IDE. It will automatically detect the gradle project and import things correctly.
 2. Enable `Annotation Processing` by going to `Build, Execution, Deployment` -> `Compiler` > `Annotation Processors` and checking 'Enable annotation processing'.
 3. Install the `Lombok Plugin`. This can be found in `Preferences` -> `Plugins`. Restart your IDE.
 4. Pravega should now compile properly.

 For eclipse, you can generate eclipse project files by running `./gradlew eclipse`.


## Documentation
For more insights into the project, please visit the following links:

- [Pravega Documentation](https://github.com/pravega/pravega/tree/master/documentation/src/docs).
- [Pravega Samples](https://github.com/pravega/pravega-samples)
- [Apache Flink](https://flink.apache.org/).
- [Apache Flink Connectors for Pravega](https://github.com/pravega/flink-connectors/wiki) provides   information on how to build and use the Flink Connector library.

# Releases

# Contributing
