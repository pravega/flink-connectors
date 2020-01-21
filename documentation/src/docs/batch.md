<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Batch Connector
The Flink Connector library for Pravega makes it possible to use a Pravega Stream as a data source and data sink in a batch program.  See the below sections for details.

## Table of Contents
- [FlinkPravegaInputFormat](#flinkpravegainputformat)
  - [Parameters](#parameters)
  - [Input Stream(s)](#input-streams)
  - [StreamCuts](#streamcuts)
  - [Parallelism](#parallelism)

- [FlinkPravegaOutputFormat](#flinkpravegaoutputformat)
  - [Parameters](#parameters)
  - [Output Stream](#output-stream)
  - [Parallelism](#parallelism)
  - [Event Routing](#event-routing)
- [Serialization](#serialization)

## FlinkPravegaInputFormat
A Pravega Stream may be used as a data source within a Flink batch program using an instance of
`FlinkPravegaInputFormat`. The input format reads events of a stream as a [`DataSet`](https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/api/java/DataSet.html) (the basic abstraction of the Flink Batch API). This input format opens the stream for batch reading, which processes stream segments in **parallel** and does not follow routing key order.

Use the [`ExecutionEnvironment::createInput`](https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/api/java/ExecutionEnvironment.html#createInput-org.apache.flink.api.common.io.InputFormat-) method to open a Pravega Stream as a `DataSet`.

### Example

```Java
// Define the Pravega configuration
PravegaConfig config = PravegaConfig.fromParams(params);

// Define the event deserializer
DeserializationSchema<EventType> deserializer = ...

// Define the input format based on a Pravega stream
FlinkPravegaInputFormat<EventType> inputFormat = FlinkPravegaInputFormat.<EventType>builder()
    .forStream(...)
    .withPravegaConfig(config)
    .withDeserializationSchema(deserializer)
    .build();

DataSource<EventType> dataSet = env.createInput(inputFormat, TypeInformation.of(EventType.class)
                                   .setParallelism(2);

```

### Parameters
A builder API is provided to construct an instance of `FlinkPravegaInputFormat`. See the table below for a summary of builder properties. Note that the builder accepts an instance of `PravegaConfig` for common configuration properties. See the [configurations](configurations.md) page for more information.

|Method                |Description|
|----------------------|-----------------------------------------------------------------------|
|`withPravegaConfig`|The Pravega client configuration, which includes connection info, security info, and a default scope.|
|`forStream`|The stream to be read from, with optional start and/or end position. May be called repeatedly to read numerous streams in parallel.|
|`withDeserializationSchema`|The deserialization schema which describes how to turn byte messages into events.|

### Input Stream(s)
Each Pravega stream exists within a scope. A scope defines a namespace for streams such that names are unique. Across scopes, streams can have the same name. For example, if we have scopes `A` and `B`, then we can have a stream called `myStream` in each one of them. We cannot have a stream with the same name in the same scope. The builder API accepts both **qualified** and **unqualified** stream names.

  - In qualified stream names, the scope is explicitly specified, e.g. `my-scope/my-stream`.
  - In unqualified stream names are assumed to refer to the default scope as set in the `PravegaConfig`.
 See the [configurations](configurations.md) page for more information on default scope.

A stream may be specified in one of three ways:

1. As a string containing a qualified name, in the form `scope/stream`.
2. As a string containing an unqualified name, in the form `stream`.  Such streams are resolved to the default scope.
3. As an instance of `io.pravega.client.stream.Stream`, e.g. `Stream.of("my-scope", "my-stream")`.

Multiple streams can be passed as parameter option (using the builder API). The [`BatchClient`](https://github.com/pravega/pravega/blob/master/client/src/main/java/io/pravega/client/batch/BatchClient.java) implementation is capable of reading from numerous streams in parallel, even across scopes.

### StreamCuts

A `StreamCut` represents a specific position in a Pravega Stream, which may be obtained from various API interactions with the Pravega client. The [`BatchClient`](https://github.com/pravega/pravega/blob/master/client/src/main/java/io/pravega/client/batch/BatchClient.java) accepts a `StreamCut` as the start and/or end position of a given stream.  For further reading on StreamCuts, please refer to documentation on [StreamCut](https://github.com/pravega/pravega/blob/master/documentation/src/docs/streamcuts.md) and [sample code](https://github.com/pravega/pravega-samples/tree/master/pravega-client-examples/src/main/java/io/pravega/example/streamcuts).

If stream cuts are not provided then the default start position requested is assumed to be the earliest available data in the stream and the default end position is assumed to be all available data in that stream as of when the job execution begins.

### Parallelism
`FlinkPravegaInputFormat` supports parallelization. Use the `setParallelism` method of `DataSet` to configure the number of parallel instances to execute.  The parallel instances consume the stream in a coordinated manner, each consuming one or more stream segments.

## FlinkPravegaOutputFormat
A Pravega Stream may be used as a data sink within a Flink batch program using an instance of `FlinkPravegaOutputFormat`. The `FlinkPravegaOutputFormat` can be supplied as a sink to the [`DataSet`](https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/api/java/DataSet.html#output-org.apache.flink.api.common.io.OutputFormat-) (the basic abstraction of the Flink Batch API).

### Example

```java
// Define the Pravega configuration
PravegaConfig config = PravegaConfig.fromParams(params);

// Define the event serializer
SerializationSchema<EventType> serializer = ...

// Define the event router for selecting the Routing Key
PravegaEventRouter<EventType> router = ...

// Define the input format based on a Pravega Stream
FlinkPravegaOutputFormat<EventType> outputFormat = FlinkPravegaOutputFormat.<EventType>builder()
    .forStream(...)
    .withPravegaConfig(config)
    .withSerializationSchema(serializer)
    .withEventRouter(router)
    .build();

ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
Collection<EventType> inputData = Arrays.asList(...);
env.fromCollection(inputData)
   .output(outputFormat);
env.execute("...");
```

### Parameter
A builder API is provided to construct an instance of `FlinkPravegaOutputFormat`. See the table below for a summary of builder properties.  Note that the builder accepts an instance of `PravegaConfig` for common configuration properties.  See the [configurations](configurations.md) page for more information.

|Method                |Description|
|----------------------|-----------------------------------------------------------------------|
|`withPravegaConfig`|The Pravega client configuration, which includes connection info, security info, and a default scope.|
|`forStream`|The stream to be written to.|
|`withSerializationSchema`|The serialization schema which describes how to turn events into byte messages.|
|`withEventRouter`|The router function which determines the Routing Key for a given event.|

### Output Stream

Each stream in Pravega is contained by a scope.  A scope acts as a namespace for one or more streams. The builder API accepts both **qualified** and **unqualified** stream names.  

  - In qualified, the scope is explicitly specified, e.g. `my-scope/my-stream`.  
  - In Unqualified stream names are assumed to refer to the default scope as set in the `PravegaConfig`.

A stream may be specified in one of three ways:

 1. As a string containing a qualified name, in the form `scope/stream`.
 2. As a string containing an unqualified name, in the form `stream`. Such streams are resolved to the default scope.
 3. As an instance of `io.pravega.client.stream.Stream`, e.g. `Stream.of("my-scope", "my-stream")`.

### Parallelism
`FlinkPravegaWriter` supports parallelization. Use the `setParallelism` method to configure the number of parallel instances to execute.

### Event Routing
Every event written to a Pravega Stream has an associated Routing Key.  The Routing Key is the basis for event ordering. See the [Pravega Concepts](http://pravega.io/docs/latest/pravega-concepts/#events) for details.

To establish the routing key for each event, provide an implementation of `PravegaEventRouter` when constructing the writer.

## Serialization
Please, see the [serialization](serialization.md) page for more information on how to use the _serializer_ and _deserializer_.
