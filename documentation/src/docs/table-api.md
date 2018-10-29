<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Table Connector
The Flink connector library for Pravega provides a table source and table sink for use with the Flink Table API.  The Table API provides a unified API for both the Flink streaming and batch environment.  See the below sections for details.

## Table of Contents
- [Table Source](#table-source)
  - [Parameters](#parameters)
  - [Custom Formats](#custom-formats)
  - [Time Attribute Support](#time-attribute-support)
- [Table Sink](#table-sink)
  - [Parameters](#parameters-1)
  - [Custom Formats](#custom-formats-1)

## Table Source
A Pravega Stream may be used as a table source within a Flink table program. The Flink Table API is oriented around Flink's `TableSchema` classes which describe the table fields.  A concrete subclass of `FlinkPravegaTableSource` is then used to parse raw stream data as `Row` objects that conform to the table schema.  The connector library provides out-of-box support for JSON-formatted data with `FlinkPravegaJsonTableSource`, and may be extended to support other formats.

#### Example

The following example uses the provided table source to read JSON-formatted events from a Pravega Stream:

```java
// Create a Flink Table environment
ExecutionEnvironment  env = ExecutionEnvironment.getExecutionEnvironment();

// Load the Pravega configuration
PravegaConfig config = PravegaConfig.fromParams(params);

String[] fieldNames = {"user", "uri", "accessTime"};

// Read data from the stream using Table reader
TableSchema tableSchema = TableSchema.builder()
        .field("user", Types.STRING())
        .field("uri", Types.STRING())
        .field("accessTime", Types.SQL_TIMESTAMP())
        .build();

FlinkPravegaJsonTableSource source = FlinkPravegaJsonTableSource.builder()
                                        .forStream(stream)
                                        .withPravegaConfig(pravegaConfig)
                                        .failOnMissingField(true)
                                        .withRowtimeAttribute("accessTime",
                                                new ExistingField("accessTime"),
                                                new BoundedOutOfOrderTimestamps(30000L))
                                        .withSchema(tableSchema)
                                        .withReaderGroupScope(stream.getScope())
                                        .build();

// (Option-1) Read table as stream data
StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
tableEnv.registerTableSource("MyTableRow", source);
String sqlQuery = "SELECT user, count(uri) from MyTableRow GROUP BY user";
Table result = tableEnv.sqlQuery(sqlQuery);
...

// (Option-2) Read table as batch data (use tumbling window as part of the query)
BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
tableEnv.registerTableSource("MyTableRow", source);
String sqlQuery = "SELECT user, " +
        "TUMBLE_END(accessTime, INTERVAL '5' MINUTE) AS accessTime, " +
        "COUNT(uri) AS cnt " +
        "from MyTableRow GROUP BY " +
        "user, TUMBLE(accessTime, INTERVAL '5' MINUTE)";
Table result = tableEnv.sqlQuery(sqlQuery);
...

```

### Parameters
A builder API is provided to construct an instance of `FlinkPravegaJsonTableSource`. See the table below for a summary of builder properties. Note that the builder accepts an instance of `PravegaConfig` for common configuration properties.  See the [configurations](configurations.md) page for more information.

Note that the table source supports both the Flink **streaming** and **batch environments**. In the streaming environment, the table source uses a [`FlinkPravegaReader`](streaming.md#flinkpravegareader) connector. In the batch environment, the table source uses a [`FlinkPravegaInputFormat`](batch.md#flinkpravegainputformat) connectors. Please see the documentation of [Streaming Connector](streaming.md) and [Batch Connector](#batch.md) to have a better understanding on the below mentioned parameter list.

|Method                |Description|
|----------------------|-----------------------------------------------------------------------|
|`withPravegaConfig`|The Pravega client configuration, which includes connection info, security info, and a default scope.|
|`forStream`|The stream to be read from, with optional start and/or end position.  May be called repeatedly to read numerous streams in parallel.|
|`uid`|The uid to identify the checkpoint state of this source.  _Applies only to streaming API._|
|`withReaderGroupScope`|The scope to store the Reader Group synchronization stream into.  _Applies only to streaming API._|
|`withReaderGroupName`|The Reader Group name for display purposes.  _Applies only to streaming API._|
|`withReaderGroupRefreshTime`|The interval for synchronizing the Reader Group state across parallel source instances.  _Applies only to streaming API._|
|`withCheckpointInitiateTimeout`|The timeout for executing a checkpoint of the Reader Group state.  _Applies only to streaming API._|
|`withSchema`|The table schema which describes which JSON fields to expect.|
|`withProctimeAttribute`|The name of the processing time attribute in the supplied table schema.|
|`withRowTimeAttribute`|supply the name of the rowtime attribute in the table schema, a TimeStampExtractor instance to extract the rowtime attribute value from the event and a `WaterMarkStratergy` to generate watermarks for the rowtime attribute.|
|`failOnMissingField`|A flag indicating whether to fail if a JSON field is missing.|

### Custom Formats
To work with stream events in a format other than JSON, extend `FlinkPravegaTableSource`. Please see the implementation of [`FlinkPravegaJsonTableSource`](https://github.com/pravega/flink-connectors/blob/master/src/main/java/io/pravega/connectors/flink/FlinkPravegaJsonTableSource.java) for more details.

### Time Attribute Support
With the use of `withProctimeAttribute` or `withRowTimeAttribute` builder method, one could supply the time attribute information of the event. The configured field must be present in the table schema and of type `Types.SQL_TIMESTAMP()`.

## Table Sink
A Pravega Stream may be used as an append-only table within a Flink table program.  The Flink Table API is oriented around Flink's `TableSchema` classes which describe the table fields.  A concrete subclass of `FlinkPravegaTableSink` is then used to write table rows to a Pravega Stream in a particular format. The connector library provides out-of-box support for JSON-formatted data with `FlinkPravegaJsonTableSource`, and may be extended to support other formats.

#### Example

The following example uses the provided table sink to write JSON-formatted events to a Pravega Stream:

```java
// Create a Flink Table environment
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

// Load the Pravega configuration
PravegaConfig config = PravegaConfig.fromParams(ParameterTool.fromArgs(args));

// Define a table (see Flink documentation)
Table table = ...

// Write the table to a Pravega Stream
FlinkPravegaJsonTableSink sink = FlinkPravegaJsonTableSink.builder()
    .forStream("sensor_stream")
    .withPravegaConfig(config)
    .withRoutingKeyField("sensor_id")
    .withWriterMode(EXACTLY_ONCE)
    .build();
table.writeToSink(sink);
```

### Parameters
A builder API is provided to construct an instance of `FlinkPravegaJsonTableSink`.  See the table below for a summary of builder properties.  Note that the builder accepts an instance of `PravegaConfig` for common configuration properties.  See the [configurations](configurations.md) wiki page for more information.

Note that the table sink supports both the Flink streaming and batch environments.  In the streaming environment, the table sink uses a [`FlinkPravegaWriter`](streaming.md#flinkpravegawriter) connector.  In the batch environment, the table sink uses a [`FlinkPravegaOutputFormat`](batch.md#flinkpravegaoutpuformat) connector.  Please see the documentation of [Streaming Connector](streaming.md) and [Batch Connector](#batch.md) to have a better understanding on the below mentioned parameter list.

|Method                |Description|
|----------------------|-----------------------------------------------------------------------|
|`withPravegaConfig`|The Pravega client configuration, which includes connection info, security info, and a default scope.|
|`forStream`|The stream to be written to.|
|`withWriterMode`|The writer mode to provide _Best-effort_, _At-least-once_, or _Exactly-once_ guarantees.|
|`withTxnTimeout`|The timeout for the Pravega Tansaction that supports the _exactly-once_ writer mode.|
|`withSchema`|The table schema which describes which JSON fields to expect.|
|`withRoutingKeyField`|The table field to use as the Routing Key for written events.|

### Custom Formats
To work with stream events in a format other than JSON, extend `FlinkPravegaTableSink`. Please see the implementation of [`FlinkPravegaJsonTableSink`](https://github.com/pravega/flink-connectors/blob/master/src/main/java/io/pravega/connectors/flink/FlinkPravegaJsonTableSink.java) for more details.
