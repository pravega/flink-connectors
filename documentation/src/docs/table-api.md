<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Table Connector
The Flink connector library for Pravega provides a table source and table sink for use with the Flink Table API.  The Table API provides a unified API for both the Flink streaming and batch environment.  See the below sections for details.

> `FlinkPravegaJsonTableSource` and `FlinkPravegaJsonTableSink` implementation has been deprecated and replaced with [ConnectorDescriptor](https://github.com/apache/flink/blob/master/flink-libraries/flink-table-common/src/main/java/org/apache/flink/table/descriptors/ConnectorDescriptor.java) / [TableFactory](https://github.com/apache/flink/blob/master/flink-libraries/flink-table-common/src/main/java/org/apache/flink/table/factories/TableFactory.java) based implementation introduced in Flink 1.6. With these changes, it is possible to use the Pravega Table API either **programmatically** (using Pravega Descriptor) or **declaratively** through YAML configuration files for the SQL client.

## Table of Contents
- [Table Source](#table-source)
  - [Parameters](#parameters)
  - [Custom Formats](#custom-formats)
  - [Time Attribute Support](#time-attribute-support)
  - [Pravega watermark (Evolving)](#pravega-watermark))
- [Table Sink](#table-sink)
  - [Parameters](#parameters-1)
  - [Custom Formats](#custom-formats-1)
- [Using SQL Client](#using-sql-client)
  - [Environment File](#environment-file)

## Table Source
A Pravega Stream may be used as a table source within a Flink table program. The Flink Table API is oriented around Flink's `TableSchema` classes which describe the table fields.  A concrete subclass of `FlinkPravegaTableSource` is then used to parse raw stream data as `Row` objects that conform to the table schema.


#### Example

The following example uses the provided table source to read JSON-formatted events from a Pravega Stream:

```java
// define table schema definition
Schema schema = new Schema()
        .field("user", Types.STRING())
        .field("uri", Types.STRING())
        .field("accessTime", Types.SQL_TIMESTAMP()).rowtime(
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
StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(execEnvRead);

StreamTableDescriptor desc = tableEnv.connect(pravega)
        .withFormat(new Json().failOnMissingField(true).deriveSchema())
        .withSchema(schema)
        .inAppendMode();

final Map<String, String> propertiesMap = DescriptorProperties.toJavaMap(desc);
final TableSource<?> source = TableFactoryService.find(StreamTableSourceFactory.class, propertiesMap)
        .createStreamTableSource(propertiesMap);

tableEnv.registerTableSource("MyTableRow", source);
String sqlQuery = "SELECT user, count(uri) from MyTableRow GROUP BY user";
Table result = tableEnv.sqlQuery(sqlQuery);
...
    
// (Option-2) Batch Source
ExecutionEnvironment execEnvRead = ExecutionEnvironment.getExecutionEnvironment();
BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(execEnvRead);
execEnvRead.setParallelism(1);

BatchTableDescriptor desc = tableEnv.connect(pravega)
        .withFormat(new Json().failOnMissingField(true).deriveSchema())
        .withSchema(schema);

final Map<String, String> propertiesMap = DescriptorProperties.toJavaMap(desc);
final TableSource<?> source = TableFactoryService.find(BatchTableSourceFactory.class, propertiesMap)
        .createBatchTableSource(propertiesMap);

tableEnv.registerTableSource("MyTableRow", source);
String sqlQuery = "SELECT ...";

Table result = tableEnv.sqlQuery(sqlQuery);
DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
...
```


```java
@deprecated 
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

> The below configurations are applicable only for the deprecated `FlinkPravegaJsonTableSource` implementation.

|Method                |Description|
|----------------------|-----------------------------------------------------------------------|
|`withSchema`|The table schema which describes which JSON fields to expect.|
|`withProctimeAttribute`|The name of the processing time attribute in the supplied table schema.|
|`withRowTimeAttribute`|supply the name of the rowtime attribute in the table schema, a TimeStampExtractor instance to extract the rowtime attribute value from the event and a `WaterMarkStratergy` to generate watermarks for the rowtime attribute.|
|`failOnMissingField`|A flag indicating whether to fail if a JSON field is missing.|

### Custom Formats
@deprecated and the steps outlined in this section is applicable only for `FlinkPravegaJsonTableSource` based implementation. Please use `Pravega` descriptor instead.

To work with stream events in a format other than JSON, extend `FlinkPravegaTableSource`. Please see the implementation of [`FlinkPravegaJsonTableSource`](https://github.com/pravega/flink-connectors/blob/master/src/main/java/io/pravega/connectors/flink/FlinkPravegaJsonTableSource.java) for more details.

### Time Attribute Support
@deprecated and the steps outlined in this section is applicable only for `FlinkPravegaJsonTableSource` based implementation. Please use `Pravega` descriptor instead.

With the use of `withProctimeAttribute` or `withRowTimeAttribute` builder method, one could supply the time attribute information of the event. The configured field must be present in the table schema and of type `Types.SQL_TIMESTAMP()`.

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
StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
Table table = tableEnv.fromDataStream(env.fromCollection(Arrays.asList(...));

Pravega pravega = new Pravega();
pravega.tableSinkWriterBuilder()
        .withRoutingKeyField("category")
        .forStream(stream)
        .withPravegaConfig(setupUtils.getPravegaConfig());

StreamTableDescriptor desc = tableEnv.connect(pravega)
        .withFormat(new Json().failOnMissingField(true).deriveSchema())
        .withSchema(new Schema().field("category", Types.STRING()).field("value", Types.INT()))
        .inAppendMode();
desc.registerTableSink("test");

final Map<String, String> propertiesMap = DescriptorProperties.toJavaMap(desc);
final TableSink<?> sink = TableFactoryService.find(StreamTableSinkFactory.class, propertiesMap)
        .createStreamTableSink(propertiesMap);

table.writeToSink(sink);
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

BatchTableDescriptor desc = tableEnv.connect(pravega)
        .withFormat(new Json().failOnMissingField(true).deriveSchema())
         .withSchema(new Schema().field("category", Types.STRING()).field("value", Types.INT()));
desc.registerTableSink("test");

final Map<String, String> propertiesMap = DescriptorProperties.toJavaMap(desc);
final TableSink<?> sink = TableFactoryService.find(BatchTableSinkFactory.class, propertiesMap)
        .createBatchTableSink(propertiesMap);

table.writeToSink(sink);
env.execute();
```

```java
@deprecated
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

> The below configurations are applicable only for the deprecated `FlinkPravegaJsonTableSink` implementation.

|Method                |Description|
|----------------------|-----------------------------------------------------------------------|
|`withSchema`|The table schema which describes which JSON fields to expect.|

### Custom Formats
@deprecated and the steps outlined in this section is applicable only for `FlinkPravegaJsonTableSink` based implementation. Please use `Pravega` descriptor instead.

To work with stream events in a format other than JSON, extend `FlinkPravegaTableSink`. Please see the implementation of [FlinkPravegaJsonTableSink](https://github.com/pravega/flink-connectors/blob/master/src/main/java/io/pravega/connectors/flink/FlinkPravegaJsonTableSink.java) for more details.

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
    type: source                            # declare if the table should be "source", "sink", or "both". If "both" provide both reader and writer configurations
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
      reader:                               # required only if type: source
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
      writer:                               # required only if type: sink
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

```yaml
tables:
  - name: sample
    type: both
    update-mode: append
    # declare the external system to connect to
    connector:
      type: pravega
      version: "1"
      metrics: true
      connection-config:
        controller-uri: "tcp://localhost:9090"
        default-scope: wVamQsOSaCxvYiHQVhRl
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
      derive-schema: true
    schema:
      - name: category
        type: VARCHAR
      - name: value
        type: INT

functions: [] 

execution:
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