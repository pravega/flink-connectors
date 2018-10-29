<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Streaming Connector

The Flink connector library for Pravega provides a data source and data sink
for use with the Flink Streaming API. See the below sections for details.

## Table of Contents
- [FlinkPravegaReader](#flinkpravegareader)
  - [Parameters](#parameters)
  - [Input Stream(s)](#input-streams)
  - [Parallelism](#parallelism)
  - [Checkpointing](#checkpointing)
  - [Timestamp Extraction / Watermark Emission](#timestamp-extraction--watermark-emission)
  - [Stream Cuts](#streamcuts)
  - [Historical Stream Processing](#historical-stream-processing)
- [FlinkPravegaWriter](#flinkpravegawriter)
  - [Parameters](#parameters-1)
  - [Parallelism](#parallelism-1)
  - [Event Routing](#event-routing)
  - [Event Time Ordering](#event-time-ordering)
  - [Writer Modes](#writer-modes)
- [Metrics](#metrics)
- [Data Serialization](#serialization)

## FlinkPravegaReader

A Pravega Stream may be used as a data source within a Flink streaming program using an instance of   `io.pravega.connectors.flink.FlinkPravegaReader`. The reader reads a given Pravega Stream (or multiple streams) as a [`DataStream`](https://ci.apache.org/projects/flink/flink-docs-stable/api/java/org/apache/flink/streaming/api/datastream/DataStream.html) (the basic abstraction of the Flink Streaming API).

Open a Pravega Stream as a DataStream using the method [`StreamExecutionEnvironment::addSource`](https://ci.apache.org/projects/flink/flink-docs-stable/api/java/org/apache/flink/streaming/api/environment/StreamExecutionEnvironment.html#addSource-org.apache.flink.streaming.api.functions.source.SourceFunction-).

#### Example
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// Define the Pravega configuration
PravegaConfig config = PravegaConfig.fromParams(params);

// Define the event deserializer
DeserializationSchema<MyClass> deserializer = ...

// Define the data stream
FlinkPravegaReader<MyClass> pravegaSource = FlinkPravegaReader.<MyClass>builder()
    .forStream(...)
    .withPravegaConfig(config)
    .withDeserializationSchema(deserializer)
    .build();
DataStream<MyClass> stream = env.addSource(pravegaSource);
```
### Parameters

A builder API is provided to construct an instance of `FlinkPravegaReader`. See the table below for a summary of builder properties.  Note that, the builder accepts an instance of `PravegaConfig` for common configuration properties.  See the [configurations](configurations.md) page for more information.

|Method                |Description|
|----------------------|-----------------------------------------------------------------------|
|`withPravegaConfig`|The Pravega client configuration, which includes connection info, security info, and a default scope.|
|`forStream`|The stream to be read from, with optional start and/or end position.  May be called repeatedly to read numerous streams in parallel.|
|`uid`|The uid to identify the checkpoint state of this source.|
|`withReaderGroupScope`|The scope to store the Reader Group synchronization stream into.|
|`withReaderGroupName`|The Reader Group name for display purposes.|
|`withReaderGroupRefreshTime`|The interval for synchronizing the Reader Group state across parallel source instances.|
|`withCheckpointInitiateTimeout`|The timeout for executing a checkpoint of the Reader Group state.|
|`withDeserializationSchema`|The deserialization schema which describes how to turn byte messages into events.|
|`enableMetrics`|true or false to enable/disable reporting Pravega metrics. Metrics is enabled by default.|

### Input Stream(s)
Each stream in Pravega is contained by a scope.  A scope acts as a namespace for one or more streams.  The `FlinkPravegaReader` is able to read from numerous streams in parallel, even across scopes.  The builder API accepts both **qualified** and **unqualified** stream names.  

  - In qualified, the scope is explicitly specified, e.g. `my-scope/my-stream`.  
  - In Unqualified stream names are assumed to refer to the default scope as set in the `PravegaConfig`.

A stream may be specified in one of three ways:

 1. As a string containing a qualified name, in the form `scope/stream`.
 2. As a string containing an unqualified name, in the form `stream`. Such streams are resolved to the default scope.
 3. As an instance of `io.pravega.client.stream.Stream`, e.g. `Stream.of("my-scope", "my-stream")`.

### Parallelism

The `FlinkPravegaReader` supports parallelization. Use the `setParallelism` method to of `Datastream` to configure the number of parallel instances to execute.  The parallel instances consume the stream in a coordinated manner, each consuming one or more stream segments.

**Note:** Coordination is achieved with the use of a Pravega Reader Group, which is based on a [State Synchronizer](http://pravega.io/docs/latest/pravega-concepts/#state-synchronizers). The Synchronizer creates a backing stream that may be manually deleted after the completion of the job.

### Checkpointing

In order to make state fault tolerant, Flink needs to **checkpoint** the state. Checkpoints allow Flink to recover state and positions in the streams to give the application the same semantics as a failure-free execution. The reader is compatible with Flink checkpoints and savepoints. The reader automatically recovers from failure by rewinding to the checkpointed position in the stream.

A **savepoint** is self-contained; it contains all information needed to resume from the correct position.

The checkpoint mechanism works as a two-step process:

   - The [master hook](https://ci.apache.org/projects/flink/flink-docs-stable/api/java/org/apache/flink/runtime/checkpoint/MasterTriggerRestoreHook.html) handler from the job manager initiates the [`triggerCheckpoint`](https://ci.apache.org/projects/flink/flink-docs-stable/api/java/org/apache/flink/runtime/checkpoint/MasterTriggerRestoreHook.html#triggerCheckpoint-long-long-java.util.concurrent.Executor-) request to  the `ReaderCheckpointHook` that was registered with the Job Manager during `FlinkPravegaReader` source initialization. The `ReaderCheckpointHook` handler notifies Pravega to checkpoint the current reader state. This is a non-blocking call which returns a `future` once Pravega readers are done with the checkpointing.
   - A `CheckPoint` event will be sent by Pravega as part of the data stream flow and on receiving the event, the `FlinkPravegaReader` will initiate [`triggerCheckpoint`](https://github.com/apache/flink/blob/master/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/checkpoint/ExternallyInducedSource.java#L73) request to effectively let Flink continue and complete the checkpoint process.

### Timestamp Extraction / Watermark Emission

Flink requires the events’ timestamps (each element in the stream needs to have its event timestamp assigned). This is achieved by accessing/extracting the timestamp from some field in the element. These are used to tell the system about progress in event time.
Pravega is not aware (or does not track) of event time and does _not_ store event timestamps or watermarks.
Nonetheless it is possible to use event time semantics via an application-specific timestamp assigner and watermark generator as described in [Flink documentation](https://ci.apache.org/projects/flink/flink-docs-release-1.3/dev/event_timestamps_watermarks.html#timestamp-assigners--watermark-generators).  

Specify an `AssignerWithPeriodicWatermarks` or `AssignerWithPunctuatedWatermarks` on the `DataStream` as normal.

Each parallel instance of the source processes one or more stream segments in parallel. Each watermark generator instance will receive events multiplexed from numerous segments. Be aware that segments are processed in parallel, and that no effort is made to order the events across segments in terms of their event time.  Also, a given segment may be reassigned to another parallel instance at any time, preserving exactly-once behavior but causing further spread in observed event times.


### StreamCuts
A `StreamCut` represents a specific position in a Pravega Stream, which may be obtained from various API interactions with the Pravega client. The `FlinkPravegaReader` accepts a `StreamCut` as the start and/or end position of a given stream. For further reading on
StreamCuts, please refer to documentation on [StreamCut](https://github.com/pravega/pravega/blob/master/documentation/src/docs/streamcuts.md) and [sample code](https://github.com/pravega/pravega-samples/tree/v0.4.2/pravega-client-examples/src/main/java/io/pravega/example/streamcuts).

#### Historical Stream Processing

Historical processing refers to processing stream data from a specific position in the stream rather than from the stream's tail.  The builder API provides an overloaded method `forStream` that accepts a `StreamCut` parameter for this purpose.

## FlinkPravegaWriter
A Pravega Stream may be used as a data sink within a Flink program using an instance of `io.pravega.connectors.flink.FlinkPravegaWriter`. Add an instance of the writer to the dataflow program using the method [`DataStream::addSink`](https://ci.apache.org/projects/flink/flink-docs-stable/api/java/org/apache/flink/streaming/api/datastream/DataStream.html#addSink-org.apache.flink.streaming.api.functions.sink.SinkFunction-).

### Example
```Java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// Define the Pravega configuration
PravegaConfig config = PravegaConfig.fromParams(params);

// Define the event serializer
SerializationSchema<MyClass> serializer = ...

// Define the event router for selecting the Routing Key
PravegaEventRouter<MyClass> router = ...

// Define the sink function
FlinkPravegaWriter<MyClass> pravegaSink = FlinkPravegaWriter.<MyClass>builder()
   .forStream(...)
   .withPravegaConfig(config)
   .withSerializationSchema(serializer)
   .withEventRouter(router)
   .withWriterMode(EXACTLY_ONCE)
   .build();

DataStream<MyClass> stream = ...
stream.addSink(pravegaSink);
```
### Parameters

A builder API is provided to construct an instance of `FlinkPravegaWriter`. See the table below for a summary of builder properties.  Note that the builder accepts an instance of `PravegaConfig` for common configuration properties.  See the [configurations](configurations.md) page for more information.

|Method                |Description|
|----------------------|-----------------------------------------------------------------------|
|`withPravegaConfig`|The Pravega client configuration, which includes connection info, security info, and a default scope.|
|`forStream`|The stream to be written to.|
|`withWriterMode`|The writer mode to provide _Best-effort, _At-least-once_, or _Exactly-once_ guarantees.|
|`withTxnLeaseRenewalPeriod`|The Transaction lease renewal period that supports the _Exactly-once_ writer mode.|
|`withSerializationSchema`|The serialization schema which describes how to turn events into byte messages.|
|`withEventRouter`|The router function which determines the Routing Key for a given event.|
|`enableMetrics`|true or false to enable/disable reporting Pravega metrics. Metrics is enabled by default.|

### Parallelism
`FlinkPravegaWriter` supports parallelization. Use the `setParallelism` method to configure the number of parallel instances to execute.

### Event Routing
Every event written to a Pravega Stream has an associated Routing Key.  The Routing Key is the basis for event ordering.  See the [Pravega Concepts](http://pravega.io/docs/latest/pravega-concepts/#events) for details.

When constructing the `FlinkPravegaWriter`, please provide an implementation of `io.pravega.connectors.flink.PravegaEventRouter` which will guarantee the event ordering. In Pravega, events are guaranteed to be ordered at the segment level.


### Event Time Ordering

For programs that use Flink's event time semantics, the connector library supports writing events in event time order. In combination with a Routing Key, this establishes a well-understood ordering for each key in the output stream.

Use the method `FlinkPravegaUtils::writeToPravegaInEventTimeOrder` to write a given `DataStream` to a Pravega Stream such that events are automatically ordered by event time (on a per-key basis). Refer [here](https://github.com/pravega/flink-connectors/blob/7971206038b51b3cf0e317e194c552c4646e5c20/src/test/java/io/pravega/connectors/flink/FlinkPravegaWriterITCase.java#L93) for sample code.

### Writer Modes
Writer modes relate to guarantees about the persistence of events emitted by the sink to a Pravega Stream.  The writer supports three writer modes:
1. **Best-effort** - Any write failures will be ignored hence there could be data loss.
2. **At-least-once** - All events are persisted in Pravega. Duplicate events
are possible, due to retries or in case of failure and subsequent recovery.
3. **Exactly-once** - All events are persisted in Pravega using a transactional approach integrated with the Flink checkpointing feature.

By default, the _At-least-once_ option is enabled and use `.withWriterMode(...)` option to override the value.

See the [Pravega documentation](http://pravega.io/docs/latest/pravega-concepts/#transactions) for details on transactional behavior.

# Metrics
Metrics are reported by default unless it is explicitly disabled using `enableMetrics(false)` option.
See [Metrics](metrics.md) page for more details on type of metrics that are reported._

# Serialization
See the [serialization](serialization.md) page for more information on how to use the _serializer_ and _deserializer_.
