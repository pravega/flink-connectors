<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Pravega Flink Connectors [![Build Status](https://travis-ci.org/pravega/flink-connectors.svg?branch=master)](https://travis-ci.org/pravega/flink-connectors)

These connectors are used to [Read and Write](http://pravega.io/docs/latest/basic-reader-and-writer/#working-with-pravega-basic-reader-and-writer) [Pravega](http://pravega.io/docs/v0.3.2/pravega-concepts/) streams with [Apache Flink](http://flink.apache.org/) stream processing applications.

This Connector is used to build end-to-end stream processing pipelines using Pravega as the stream storage and message bus. It uses Apache Flink for performing computations over the streams.


## Features and Highlights

  - **Exactly-Once Semantics:** Pravega ensures that each event is delivered and processed exactly once, with exact ordering guarantees, despite failures in clients, servers or the network. Thus, offering **[Exactly-once processing guarantees]**(http://pravega.io/docs/v0.3.2/key-features/#exactly-once-semantics)for both reader and writer, supporting **end-to-end exactly-once processing pipelines**.

  - Possess seamless integration with [Flink's checkpoints and savepoints](https://github.com/pravega/flink-connectors/wiki/Streaming-Connector#checkpointing).

  - Allows parallel readers and writers by supporting high throughput and low latency processing.



## Documentation
For more insights into the project, please visit the following links:

- [Pravega Documentation](https://github.com/pravega/pravega/tree/master/documentation/src/docs).
- [Pravega Samples](https://github.com/pravega/pravega-samples)
- [Apache Flink](https://flink.apache.org/).
- [Apache Flink Connectors for Pravega](https://github.com/pravega/flink-connectors/wiki).


See the [Project Wiki](https://github.com/pravega/flink-connectors/wiki) for documentation on how to build and use the Flink Connector library.
