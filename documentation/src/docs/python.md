# Python API for the connector

This Pravega connector of Python API provides a data source and data sink for Flink streaming jobs.

**DISCLAIMER: This python wrapper is an IMPLEMENTATION REFERENCE and is not meant for out-of-box usage.**

[TOC]

## PravegaConfig

A top-level config object, `PravegaConfig`, is provided to establish a Pravega context for the Flink connector.

```python
from pravega_config import PravegaConfig

pravega_config = PravegaConfig(CONTROLLER_URI, SCOPE)
```

|parameter|type|required|default value|description|
|-|-|-|-|-|
|uri|str|Yes|N/A|The Pravega controller RPC URI.|
|scope|str|Yes|N/A|The self-defined Pravega scope.|
|schema_registry_uri|str|No|None|The Pravega schema registry URI.|
|trust_store|str|No|None|The truststore value.|
|default_scope|str|No|None|The default Pravega scope, to resolve unqualified stream names and to support reader groups.|
|credentials|DefaultCredentials|No|None|The Pravega credentials to use.|
|validate_hostname|bool|No|True|TLS hostname validation.|

For more details about *Default Scope*? See [the java doc](https://github.com/pravega/flink-connectors/blob/master/documentation/src/docs/configurations.md#understanding-the-default-scope).

## StreamCut

A `StreamCut` represents a specific position in a Pravega Stream, which may be obtained from various API interactions with the Pravega client. The `FlinkPravegaReader` accepts a `StreamCut` as the start and/or end position of a given stream. For further reading on `StreamCuts`, please refer to documentation on [StreamCut](http://pravega.io/docs/latest/streamcuts/) and [sample code(java only)](https://github.com/pravega/pravega-samples/tree/master/pravega-client-examples/src/main/java/io/pravega/example/streamcuts).

A `StreamCut` object could be constructed from the `from_base64` class method where a base64 str is passed as the only parameter.

By default, the `FlinkPravegaReader` will pass the `UNBOUNDED` `StreamCut` which let the reader read from the HEAD to the TAIL.

For more details about *Historical Stream Processing*? See [the java doc](https://github.com/pravega/flink-connectors/blob/master/documentation/src/docs/streaming.md#historical-stream-processing).

## FlinkPravegaReader

Use `FlinkPravegaReader` as a datastream source. Could be added by `env.add_source`.

```python
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment

from pravega_config import PravegaConfig
from pravega_reader import FlinkPravegaReader

env = StreamExecutionEnvironment.get_execution_environment()

pravega_config = PravegaConfig(CONTROLLER_URI, SCOPE)
pravega_reader = FlinkPravegaReader(
    stream=STREAM,
    pravega_config=pravega_config,
    deserialization_schema=SimpleStringSchema())

ds = env.add_source(pravega_reader)
```

|parameter|type|required|default value|description|
|-|-|-|-|-|
|stream|Union[str, Stream]|Yes|N/A|The stream to be read from.|
|pravega_config|PravegaConfig|Yes|N/A|Set the Pravega client configuration, which includes connection info, security info, and a default scope.|
|deserialization_schema|DeserializationSchema|Yes|N/A|The deserialization schema which describes how to turn byte messages into events.|
|start_stream_cut|StreamCut|No|StreamCut.UNBOUNDED|Read from the given start position in the stream.|
|end_stream_cut|StreamCut|No|StreamCut.UNBOUNDED|Read to the given end position in the stream.|
|enable_metrics|bool|No|True|Pravega reader metrics.|
|uid|str|No|None(random generated uid on java side)|The uid to identify the checkpoint state of this source.|
|reader_group_scope|str|No|pravega_config.default_scope|The scope to store the Reader Group synchronization stream into.|
|reader_group_name|str|No|None(auto-generated name on java side)|The Reader Group name for display purposes.|
|reader_group_refresh_time|timedelta|No|None(3 seconds on java side)|The interval for synchronizing the Reader Group state across parallel source instances.|
|checkpoint_initiate_timeout|timedelta|No|None(5 seconds on java side)|The timeout for executing a checkpoint of the Reader Group state.|
|event_read_timeout|timedelta|No|None(1 second on java side)|Sets the timeout for the call to read events from Pravega. After the timeout expires (without an event being returned), another call will be made.|
|max_outstanding_checkpoint_request|int|No|None(3 on java side)|Configures the maximum outstanding checkpoint requests to Pravega.|

For more details about concepts like *Reader Parallelism* and *Checkpointing*? See [the java doc](https://github.com/pravega/flink-connectors/blob/master/documentation/src/docs/streaming.md#input-streams).

## FlinkPravegaWriter

Use `FlinkPravegaWriter` as a datastream sink. Could be added by `env.add_sink`.

```python
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment

from pravega_config import PravegaConfig
from pravega_writer import FlinkPravegaWriter

env = StreamExecutionEnvironment.get_execution_environment()

pravega_config = PravegaConfig(CONTROLLER_URI, SCOPE)
pravega_writer = FlinkPravegaWriter(stream=STREAM,
                                    pravega_config=pravega_config,
                                    serialization_schema=SimpleStringSchema())

ds = env.add_sink(pravega_reader)
```

|parameter|type|required|default value|description|
|-|-|-|-|-|
|stream|Union[str, Stream]|Yes|N/A|Add a stream to be read by the source, from the earliest available position in the stream.|
|pravega_config|PravegaConfig|Yes|N/A|Set the Pravega client configuration, which includes connection info, security info, and a default scope.|
|serialization_schema|SerializationSchema|Yes|N/A|The serialization schema which describes how to turn events into byte messages.|
|enable_metrics|bool|No|True|Pravega writer metrics.|
|writer_mode|PravegaWriterMode|No|PravegaWriterMode.ATLEAST_ONCE|The writer mode to provide *Best-effort*, *At-least-once*, or *Exactly-once* guarantees.|
|enable_watermark|bool|No|False|Emit Flink watermark in event-time semantics to Pravega streams.|
|txn_lease_renewal_period|timedelta|No|None(30 seconds on java side)|Report Pravega metrics.|

For more details about concepts like *Watermark* and *Writer Modes*? See [the java doc](https://github.com/pravega/flink-connectors/blob/master/documentation/src/docs/streaming.md#writer-parallelism).

## Metrics

Metrics are reported by default unless it is explicitly disabled using enable_metrics(False) option. See [Metrics](https://github.com/pravega/flink-connectors/blob/master/documentation/src/docs/metrics.md) page for more details on type of metrics that are reported.

Metrics is also gatherable by Python code. See [Metrics](https://ci.apache.org/projects/flink/flink-docs-stable/docs/dev/python/table/metrics/) page of PyFlink for more information.

## Serialization

See the [serialization](https://github.com/pravega/flink-connectors/blob/master/documentation/src/docs/serialization.md) page for more information on how to use the java serializer and deserializer.

See the [Data Types](https://ci.apache.org/projects/flink/flink-docs-stable/docs/dev/python/datastream/data_types/) page of PyFlink for more information.
