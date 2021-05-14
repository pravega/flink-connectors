<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Table Connector
The Flink connector library for Pravega provides a table source and table sink for use with the Flink Table API. 
The Table API provides a unified table source API for both the Flink streaming and batch environment, and also sink for the Flink streaming environment.

It is possible to treat the Pravega streams as tables with the help of Flink.

See the below sections for details.

## Table of Contents
- [Introduction](#introduction)
- [How to create a table](#how-to-create-a-table)
- [Connector options](#connector-options)
- [Features](#features)
  - [Batch and Streaming read](#batch-and-streaming-read)
  - [Specify start and end streamcut](#specify-start-and-end-streamcut)
  - [Changelog Source](#changelog-source)
  - [Routing key by column](#routing-key-by-column)
  - [Consistency guarantees](#consistency-guarantees)
- [Useful Flink links](#useful-flink-links)


## Introduction
Before Flink 1.10 connector, the connector has implemented Flink legacy `TableFactory` interface to support table mapping, 
and provided `FlinkPravegaTableSource` and `FlinkPravegaTableSink` to read and write Pravega as Flink tables via a Pravega descriptor.

Since Flink 1.11 connector, as Flink introduces a new Table API with [FLIP-95](https://cwiki.apache.org/confluence/display/FLINK/FLIP-95%3A+New+TableSource+and+TableSink+interfaces), 
we integrate Flink `Factory` interface and provided `FlinkPravegaDynamicTableSource` and `FlinkPravegaDynamicTableSink` to simplify the application coding. 

Note that the legacy table API is deprecated and will be removed in the future releases, we strongly suggest users to switch to the new table API.
We will focus on the new table API introduction in the document below, please refer to the documentation of older versions if you want to check the legacy table API.

Pravega table source supports both the Flink **streaming** and **batch** environments.
Pravega table sink is an append-only table sink, it does NOT support upsert/retract output.

## How to create a table
Pravega Stream can be used as a table source/sink within a Flink table program.
The example below shows how to create a table connecting a Pravega stream as both source and sink:

```sql
CREATE TABLE user_behavior (
    user_id STRING,
    item_id BIGINT,
    category_id BIGINT,
    behavior STRING,
    log_ts TIMESTAMP(3),
    ts as log_ts + INTERVAL '1' SECOND,
    watermark for ts as ts
    )
WITH (
    'connector' = 'pravega'
    'controller-uri' = 'tcp://localhost:9090',
    'scope' = 'scope',
    'scan.execution.type' = 'streaming',
    'scan.streams' = 'stream',
    'sink.stream' = 'stream',
    'sink.routing-key.field.name' = 'user_id',
    'format' = 'json'
    )
```

## Connector options
| Option                                                 | Required            | Default       | Type         | Description                                                                                                   |
|--------------------------------------------------------|---------------------|---------------|--------------|---------------------------------------------------------------------------------------------------------------|
| connector                                              | required            | (none)        | String       | Specify what connector to use, here should be 'pravega'                                                       |
| controller-uri                                         | required            | (none)        | String       | Pravega controller URI                                                                                        |
| security.auth-type                                     | optional            | (none)        | String       | Static authentication/authorization type for security                                                         |
| security.auth-token                                    | optional            | (none)        | String       | Static authentication/authorization token for security                                                        |
| security.validate-hostname                             | optional            | (none)        | Boolean      | If host name validation should be enabled when TLS is enabled                                                 |
| security.trust-store                                   | optional            | (none)        | String       | Trust Store for Pravega client                                                                                |
| scan.execution.type                                    | optional            | streaming     | String       | Execution type for scan source. Valid values are 'streaming', 'batch'.                                        |
| scan.reader-group.name                                 | optional            | (none)        | String       | Pravega reader group name                                                                                     |
| scan.streams                                           | required for source | (none)        | List<String> | Semicolon-separated list of stream names from which the table is read.                                        |
| scan.start-streamcuts                                  | optional            | (none)        | List<String> | Semicolon-separated list of base64 encoded strings for start streamcuts, begin of the stream if not specified |
| scan.end-streamcuts                                    | optional            | (none)        | List<String> | Semicolon-separated list of base64 encoded strings for end streamcuts, unbounded end if not specified         |
| scan.reader-group.max-outstanding-checkpoint-request   | optional            | 3             | Integer      | Maximum outstanding checkpoint requests to Pravega                                                            |
| scan.reader-group.refresh.interval                     | optional            | 3 s           | Duration     | Refresh interval for reader group                                                                             |
| scan.event-read.timeout.interval                       | optional            | 1 s           | Duration     | Timeout for the call to read events from Pravega                                                              |
| scan.reader-group.checkpoint-initiate-timeout.interval | optional            | 5 s           | Duration     | Timeout for call that initiates the Pravega checkpoint                                                        |
| sink.stream                                            | required for sink   | (none)        | String       | Stream name to which the table is written                                                                     |
| sink.semantic                                          | optional            | at-least-once | String       | Semantic when commit. Valid values are 'at-least-once', 'exactly-once', 'best-effort'                         |
| sink.txn-lease-renewal.interval                        | optional            | 30 s          | Duration     | Transaction lease renewal period, valid for exactly-once semantic.                                            |
| sink.enable.watermark-propagation                      | optional            | false         | Boolean      | If watermark propagation should be enabled from Flink table to Pravega stream                                 |
| sink.routing-key.field.name                            | optional            | (none)        | String       | Field name to use as a Pravega event routing key, field type must be STRING, random routing if not specified. |

## Features

### Batch and Streaming read

`scan.execution.type` can be specified as user's choice to perform batch read or streaming read.
In the streaming environment, the table source uses a [`FlinkPravegaReader`](streaming.md#flinkpravegareader) connector.
In the batch environment, the table source uses a [`FlinkPravegaInputFormat`](batch.md#flinkpravegainputformat) connector.
Please see the documentation of [Streaming Connector](streaming.md) and [Batch Connector](#batch.md) to have a better understanding on the below mentioned parameter list.

### Specify start and end streamcut

A `StreamCut` represents a consistent position in the stream, and can be fetched from other applications uses Pravega client through checkpoints or custom defined index. 
`scan.start-streamcuts` and `scan.end-streamcuts` can be specified to perform bounded read and "start-at-some-point" read for Pravega streams.
Pravega source supports read from multiple streams, and if read from multiple streams, please make sure the order of the streamcuts keeps the same as the order of the streams.

### Read metadata from pravega

The connector could provide event metadata (e.g. event pointer) for each event.
This would facilitate the development of jobs that care about the stream position of the event data, e.g. for indexing purposes.

Metadata `event_pointer` is an array of bytes that could be read from the pravega via the connector.
To read it, simply add the `METADATA VIRTUAL` keyword to the end of the `event_pointer` field.

```sql
CREATE TABLE test (
    key STRING,
    event_pointer BYTES METADATA VIRTUAL
    )
WITH (
    'connector' = 'pravega'
    'controller-uri' = 'tcp://localhost:9090',
    'scope' = 'scope',
    'scan.streams' = 'stream',
    'format' = 'json'
    )
```

After getting the bytes from the connector, it can be used to retrieve the original data from the pravega.

To get the data:
1. Convert the `byte[]` to `ByteBuffer`: `ByteBuffer#wrap`
2. Get the event pointer: `EventPointer#fromBytes`
3. Get the data: `EventStreamReader#fetchEvent`

### Changelog Source

If messages in Pravega stream is change event captured from other databases using CDC tools, then you can use a CDC format to interpret messages as INSERT/UPDATE/DELETE messages into Flink SQL system.
Flink provides two CDC formats [`debezium-json`](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/formats/debezium.html) and [`canal-json`](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/formats/canal.html) to interpret change events captured by Debezium and Canal.
The changelog source is a very useful feature in many cases, such as synchronizing incremental data from databases to other systems, auditing logs, materialized views on databases, temporal join changing history of a database table and so on.
See more about how to use the CDC formats in [`debezium-json`](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/formats/debezium.html) and [`canal-json`](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/formats/canal.html)

### Routing key by column

Pravega writers can use domain specific meaningful Routing Keys (like customer ID, Timestamp, Machine ID, etc.) to group similar together and make such parallelism with segment scaling. 
Pravega makes ordering guarantees in terms of routing keys.
Pravega sink supports event routing according to a certain event field by specifying `sink.routing-key.field.name`. This field type must be `STRING`, and it will be random routing if not specified.

### Consistency guarantees

By default, a Pravega sink ingests data with at-least-once guarantees if the query is executed with checkpointing enabled.
`sink.semantic: exactly-once` can be specified to turn on the transactional writes with exactly-once guarantees.

## Useful Flink links

Users can try with Pravega table APIs quickly though Flink SQL client. Here is some tutorial to setup the environment.
https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/sqlClient.html

The usage and definition of time attribute and WATERMARK schema can be referred in:
https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/streaming/time_attributes.html