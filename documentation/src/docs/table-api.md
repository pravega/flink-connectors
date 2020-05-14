<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Table Connector
The Flink connector library for Pravega provides a table source and table sink for use with the Flink Table API.  The Table API provides a unified API for both the Flink streaming and batch environment.

It is possible to use the Pravega Table API either **programmatically** (using Pravega Descriptor) or **declaratively** through YAML configuration files for the SQL client.

See the below sections for details.
## Table of Contents
- [Table Source](#table-source)
  - [Parameters](#parameters)
  - [Pravega watermark (Evolving)](#pravega-watermark))
- [Table Sink](#table-sink)
  - [Parameters](#parameters-1)
- [Using SQL Client](#using-sql-client)
  - [Environment File](#environment-file)

## Table Source
A Pravega Stream may be used as a table source within a Flink table program. The Flink Table API is oriented around Flink's `TableSchema` classes which describe the table fields.  A concrete subclass of `FlinkPravegaTableSource` is then used to parse raw stream data as `Row` objects that conform to the table schema.


#### Example

The following example uses the provided table source to read JSON-formatted events from a Pravega Stream:

```java
// define table schema definition
Schema schema = new Schema()
                .field("user", DataTypes.STRING())
                .field("uri", DataTypes.STRING())
                .field("accessTime", DataTypes.TIMESTAMP(3)).rowtime(
                        new Rowtime().timestampsFromField("accessTime")
                                       .watermarksPeriodicBounded(30000L));

// define pravega reader configurations using Pravega descriptor
Pravega pravega = new Pravega();
pravega.tableSourceReaderBuilder()
        .withReaderGroupScope(stream.getScope())
        .forStream(stream)
        .withPravegaConfig(pravegaConfig);


// (Option-1) Streaming Source
StreamExecutionEnvironment execEnvRead = StreamExecutionEnvironment.getExecutionEnvironment();
// Old Planner
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(execEnvRead);
// Blink Planner, this is recommended.(Difference between 2 planners can be referred in https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/common.html#main-differences-between-the-two-planners)
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(execEnvRead,
                EnvironmentSettings.newInstance()
                        .useBlinkPlanner()
                        .inStreamingMode()
                        .build());

tableEnv.connect(pravega)
                .withFormat(new Json().failOnMissingField(true))
                .withSchema(schema)
                .inAppendMode()
               // In Flink 1.10, createTemporaryTable can be used here as well unless time attributes are added into the schema.
               // (ref. There are some known issues in Flink https://issues.apache.org/jira/browse/FLINK-16160)
                .registerTableSource("MyTableRow");


String sqlQuery = "SELECT user, count(uri) from MyTableRow GROUP BY user";
Table result = tableEnv.sqlQuery(sqlQuery);
...
    
// (Option-2) Batch Source
ExecutionEnvironment execEnvRead = ExecutionEnvironment.getExecutionEnvironment();
BatchTableEnvironment tableEnv = BatchTableEnvironment.create(execEnvRead);
execEnvRead.setParallelism(1);

tableEnv.connect(pravega)
                .withFormat(new Json().failOnMissingField(true))
                .withSchema(schema)
                .inAppendMode()
                .registerTableSource("MyTableRow");

String sqlQuery = "SELECT ...";

Table result = tableEnv.sqlQuery(sqlQuery);
DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
...
```
```
### Parameters
A builder API is provided to construct an concrete subclass of `FlinkPravegaTableSource`. See the table below for a summary of builder properties. Note that the builder accepts an instance of `PravegaConfig` for common configuration properties.  See the [configurations](configurations.md) page for more information.

Note that the table source supports both the Flink **streaming** and **batch environments**. In the streaming environment, the table source uses a [`FlinkPravegaReader`](streaming.md#flinkpravegareader) connector. In the batch environment, the table source uses a [`FlinkPravegaInputFormat`](batch.md#flinkpravegainputformat) connector. Please see the documentation of [Streaming Connector](streaming.md) and [Batch Connector](#batch.md) to have a better understanding on the below mentioned parameter list.

|Method                |Description|
|----------------------|-----------------------------------------------------------------------|
|`withPravegaConfig`|The Pravega client configuration, which includes connection info, security info, and a default scope.|
|`forStream`|The stream to be read from, with optional start and/or end position.  May be called repeatedly to read numerous streams in parallel.|
|`uid`|The uid to identify the checkpoint state of this source.  _Applies only to streaming API._|
|`withReaderGroupScope`|The scope to store the Reader Group synchronization stream into.  _Applies only to streaming API._|
|`withReaderGroupName`|The Reader Group name for display purposes.  _Applies only to streaming API._|
|`withReaderGroupRefreshTime`|The interval for synchronizing the Reader Group state across parallel source instances.  _Applies only to streaming API._|
|`withCheckpointInitiateTimeout`|The timeout for executing a checkpoint of the Reader Group state.  _Applies only to streaming API._|
|`withTimestampAssigner`| (Evolving) The `AssignerWithTimeWindows` implementation to implementation which describes the event timestamp and Pravega watermark strategy in event time semantics.  _Applies only to streaming API._|

### Pravega watermark (Evolving)
Pravega watermark for Table API Reader depends on the underlying DataStream settings. The following example shows how to read data with watermark by a table source.
```java
// A user-defined implementation of `AssignerWithTimeWindows`, the event type should be `Row`
public static class MyAssigner extends LowerBoundAssigner<Row> {
    public MyAssigner() {}

    @Override
    public long extractTimestamp(Row element, long previousElementTimestamp) {
        // The third attribute of the element is the event timestamp
        return (long) element.getField(2);
    }
}

Pravega pravega = new Pravega();
pravega.tableSourceReaderBuilder()
        // Assign the watermark in the source
        .withTimestampAssigner(new MyAssigner())
        .withReaderGroupScope(stream.getScope())
        .forStream(stream)
        .withPravegaConfig(pravegaConfig);

final ConnectTableDescriptor tableDesc = new TestTableDescriptor(pravega)
        .withFormat(...)
        .withSchema(
                new Schema()
                        .field(...)
                        // Use the timestamp and Pravega watermark defined in the source
                        .rowtime(new Rowtime()
                                .timestampsFromSource()
                                .watermarksFromSource()
                        ))
        .inAppendMode();
```

## Table Sink
A Pravega Stream may be used as an append-only table within a Flink table program.  The Flink Table API is oriented around Flink's `TableSchema` classes which describe the table fields.  A concrete subclass of `FlinkPravegaTableSink` is then used to write table rows to a Pravega Stream in a particular format.

#### Example

The following example uses the provided table sink to write JSON-formatted events to a Pravega Stream:

```java
// (Option-1) Streaming Sink
StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment().setParallelism(1);
// Old Planner
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
// Blink Planner, this is recommended.(Difference between 2 planners can be referred in https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/common.html#main-differences-between-the-two-planners)
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance()
                        .useBlinkPlanner()
                        .inStreamingMode()
                        .build());

Table table = tableEnv.fromDataStream(env.fromCollection(Arrays.asList(...));

Pravega pravega = new Pravega();
pravega.tableSinkWriterBuilder()
        .withRoutingKeyField("category")
        .forStream(stream)
        .withPravegaConfig(setupUtils.getPravegaConfig());

tableEnv.connect(pravega)
                .withFormat(new Json().failOnMissingField(true))
                .withSchema(new Schema().field("category", DataTypes.STRING()).
                        field("value", DataTypes.INT()))
                .registerTableSink("PravegaSink");

table.insertInto("PravegaSink");
env.execute();

// (Option-2) Batch Sink
ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
Table table = tableEnv.fromDataSet(env.fromCollection(Arrays.asList(...));

Pravega pravega = new Pravega();
pravega.tableSinkWriterBuilder()
        .withRoutingKeyField("category")
        .forStream(stream)
        .withPravegaConfig(setupUtils.getPravegaConfig());

tableEnv.connect(pravega)
                .withFormat(new Json().failOnMissingField(true))
                .withSchema(new Schema().field("category", DataTypes.STRING()).
                        field("value", DataTypes.INT()))
                .registerTableSink("PravegaSink");

table.insertInto("PravegaSink");
env.execute();
```

### Parameters
A builder API is provided to construct a concrete subclass of `FlinkPravegaTableSink`.  See the table below for a summary of builder properties.  Note that the builder accepts an instance of `PravegaConfig` for common configuration properties.  See the [configurations](configurations.md) page for more information.

Note that the table sink supports both the Flink streaming and batch environments.  In the streaming environment, the table sink uses a [FlinkPravegaWriter](streaming.md#flinkpravegawriter) connector.  In the batch environment, the table sink uses a [FlinkPravegaOutputFormat](batch.md#flinkpravegaoutpuformat) connector.  Please see the documentation of [Streaming Connector](streaming.md) and [Batch Connector](#batch.md) to have a better understanding on the below mentioned parameter list.

|Method                |Description|
|----------------------|-----------------------------------------------------------------------|
|`withPravegaConfig`|The Pravega client configuration, which includes connection info, security info, and a default scope.|
|`forStream`|The stream to be written to.|
|`withWriterMode`|The writer mode to provide _Best-effort_, _At-least-once_, or _Exactly-once_ guarantees.|
|`withTxnTimeout`|The timeout for the Pravega Tansaction that supports the _exactly-once_ writer mode.|
|`withRoutingKeyField`|The table field to use as the Routing Key for written events.|
|`enableWatermark`|true or false to enable/disable the event-time watermark emitting into Pravega stream.|

## Using SQL Client
[Flink Sql Client](https://ci.apache.org/projects/flink/flink-docs-master/dev/table/sqlClient.html) was introduced in Flink 1.6 which aims at providing an easy way of writing, debugging, and submitting table programs to a Flink cluster without a single line of Java or Scala code. The SQL Client CLI allows for retrieving and visualizing real-time results from the running distributed application on the command line. 

It is now possible to access Pravega streams using standard SQL commands through Flink's SQL client. To do so, the following files have to copied to Flink cluster library `$FLINK_HOME/lib` path
- Pravega connector jar
- Flink JSON jar (to serialize/deserialize data in json format)
- Flink Avro jar (to serialize/deserialize data in avro format)

Flink format jars can be downloaded from [maven central repository](http://central.maven.org/maven2/org/apache/flink).

In a nutshell, here is what we need to do to use Flink SQL client with Pravega.
1. Download Flink binary version supported by the connector.
2. Make sure to copy flink-table*.jar and flink-sql-client*.jar from $FLINK-HOME/opt/ to $FLINK-HOME/lib/ location.
3. Copy Flink format jars (json, avro) from maven central to $FLINK-HOME/lib/ location.
4. Copy Flink Pravega connector jar file to $FLINK-HOME/lib/ location. 
5. Prepare SQL client configuration file (that contains Pravega connector descriptor configurations). Make sure to create any Pravega streams that you will be accessing from SQL client shell ahead of time. 
6. Run SQL client shell in embedded mode using the command `$FLINK-HOME/bin/sql-client.sh embedded -d <SQL_configuration_file>`
7. Run `SELECT 'Hello World'` from SQL client shell and make sure it does not throw any errors. It should show an empty results screen if there are no errors.
8. After these steps, you could run SQL commands from the SQL client shell prompt to interact with Pravega. 

For more details on how to setup, configure and access the SQL client shell, please follow the [getting started](https://ci.apache.org/projects/flink/flink-docs-master/dev/table/sqlClient.html#getting-started) documentation.

### Environment File
The YAML configuration file schema for providing Pravega table API specific connector configuration is provided below.

```yaml
tables:
  - name: sample                            # name the new table
    type: source-table                      # declare if the table should be "source-table", "sink-table", or "source-sink-table". If "source-sink-table" provide both reader and writer configurations
    update-mode: append                     # specify the update-mode *only* for streaming tables

    # declare the external system to connect to
    connector:
      type: pravega
      version: "1"
      metrics:                              # optional (true|false)
      connection-config:
        controller-uri:                     # mandatory
        default-scope:                      # optional (assuming reader or writer provides scope)
        security:                           # optional
          auth-type:                        # optional (base64 encoded string)
          auth-token:                       # optional (base64 encoded string)
          validate-hostname:                # optional (true|false)
          trust-store:                      # optional (truststore filename)
      reader:                               # required only if type: source-table
        stream-info:
          - scope: test                     # optional (uses default-scope value or else throws error)
            stream: stream1                 # mandatory
            start-streamcut:                # optional (base64 encoded string)
            end-streamcut:                  # optional (base64 encoded string)
          - scope: test                     # repeating info to provide multiple stream configurations
            stream: stream2
            start-streamcut:
            end-streamcut:
        reader-group:                       # optional
          uid:                              # optional
          scope:                            # optional (uses default-scope or else throws error)
          name:                             # optional
          refresh-interval:                 # optional (long milliseconds)
          event-read-timeout-interval:      # optional (long milliseconds)
          checkpoint-initiate-timeout-interval:  # optional (long milliseconds)
      writer:                               # required only if type: sink-table
        scope: foo                          # optional (uses default-scope value)
        stream: bar                         # mandatory
        mode:                               # optional (exactly_once | atleast_once)
        txn-lease-renewal-interval:         # optional (long milliseconds)
        routingkey-field-name:              # mandatory (provide field name from schema that has to be used as routing key)

    # declare a format for this system (refer flink documentation for details) 
    format:

    # declare the schema of the table (refer flink documentation for details)
    schema:
```

### Sample Environment File
Here is a sample environment file for reference which can be used as a source as well as sink to read from and write data into Pravega as table records

But the stream `streamX` should be created in advance to have it working.

```yaml
tables:
  - name: sample
    type: source-sink-table
    update-mode: append
    # declare the external system to connect to
    connector:
      type: pravega
      version: "1"
      metrics: true
      connection-config:
        controller-uri: "tcp://localhost:9090"
        default-scope: test-scope
      reader:
        stream-info:
          - stream: streamX
      writer:
        stream: streamX
        mode: atleast_once
        txn-lease-renewal-interval: 10000
        routingkey-field-name: category
    format:
      type: json
      fail-on-missing-field: true
    schema:
      - name: category
        data-type: STRING
      - name: value
        data-type: INT
      - name: timestamp
        data-type: TIMESTAMP(3)

functions: [] 

execution:
  # either 'old' (default) or 'blink',blink planner is recommended.Please refer to https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/common.html#main-differences-between-the-two-planners
  planner: blink
  # 'batch' or 'streaming' execution
  type: streaming
  # allow 'event-time' or only 'processing-time' in sources
  time-characteristic: event-time
  # interval in ms for emitting periodic watermarks
  periodic-watermarks-interval: 200
  # 'changelog' or 'table' presentation of results
  result-mode: table
  # parallelism of the program
  parallelism: 1
  # maximum parallelism
  max-parallelism: 128
  # minimum idle state retention in ms
  min-idle-state-retention: 0
  # maximum idle state retention in ms
  max-idle-state-retention: 0

deployment:
  # general cluster communication timeout in ms
  response-timeout: 5000
  # (optional) address from cluster to gateway
  gateway-address: ""
  # (optional) port from cluster to gateway
  gateway-port: 0

```

### Sample DDL

Here is a sample DDL for reference which can be used as a source as well as sink to read from and write data into Pravega as table records.

But the stream `streamX` should be created in advance to have it working.

```
CREATE TABLE sample (
  category STRING,
  cnt INT,
  ts TIMESTAMP(3),
  WATERMARK FOR ts as ts - INTERVAL '5' SECOND
) WITH (
  'update-mode' = 'append',
  'connector.type' = 'pravega',
  'connector.version' = '1',
  'connector.metrics' = 'true',
  'connector.connection-config.controller-uri' = 'tcp://localhost:9090',
  'connector.connection-config.default-scope' = 'test',
  'connector.reader.stream-info.0.stream' = 'streamX',
  'connector.writer.stream' = 'streamX',
  'connector.writer.mode' = 'atleast_once',
  'connector.writer.txn-lease-renewal-interval' = '10000',
  'connector.writer.routingkey-field-name' = 'category'
  'format.type' = 'json',
  'format.fail-on-missing-field' = 'false'
)

```

The usage and definition of time attribute and DDL and usage of WATERMARK schema can be referred in:

* Time Attribute

https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/streaming/time_attributes.html

* DDL Usage

https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/sql/create.html#create-table


### Timestamp format issue with Flink SQL

Please refer to [Table formats documentation](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/connect.html)

Here is the valid data example with the above sample yaml.

`{"category": "test-category", "value": 310884, "timestamp": "2017-11-27T00:00:00Z"}`