<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Pravega Flink Connectors [![Build Status](https://travis-ci.org/pravega/flink-connectors.svg?branch=master)](https://travis-ci.org/pravega/flink-connectors)

Connectors to read and write [Pravega](http://pravega.io/) streams with [Apache Flink](http://flink.apache.org/) stream processing applications.

Build end-to-end stream processing pipelines that use Pravega as the stream storage and message bus, and Apache Flink for computation over the streams.


## Features & Highlights

  - **Exactly-once processing guarantees** for both reader and writer, supporting **end-to-end exactly-once processing pipelines**

  - Seamless integration with Flink's checkpoints & savepoints

  - Parallel readers and writers supporting high throughput and low latency processing

## Documentation
See the [Project Wiki](https://github.com/pravega/flink-connectors/wiki) for documentation on how to build and use the Flink Connector library.
