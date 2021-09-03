<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0
-->

# Pravega Flink Connectors for Python

[![Build Status](https://img.shields.io/github/workflow/status/pravega/flink-connectors/build)](https://github.com/pravega/flink-connectors/actions/workflows/build.yml?query=branch%3Amaster) [![Issue Tracking](https://img.shields.io/github/issues/pravega/flink-connectors)](https://github.com/pravega/flink-connectors/issues) [![License](https://img.shields.io/github/license/pravega/flink-connectors)](https://github.com/pravega/flink-connectors/blob/master/LICENSE) [![Downloads](https://img.shields.io/github/downloads/pravega/flink-connectors/total)](https://github.com/pravega/flink-connectors/releases) [![docs](https://img.shields.io/static/v1?label=docs&message=see-latest&color=blue)](https://github.com/pravega/flink-connectors/tree/master/documentation/src/docs)

👏🎉 Welcome to the Python world of Pravega Flink Connectors!

🧾 This page is all about how to use the connectors in `PyFlink`.

🚨 **DISCLAIMER: This python wrapper is an IMPLEMENTATION REFERENCE and is not meant for out-of-box usage.** See below [Technical Details](#Technical-Details) for more information.

---

We assume you know some basic ideas about `Pravega`, `Python`, `PyFlink` and is eager to use Pravega as the streaming storage for batch and/or streaming workloads of Flink.
If not, see the below [Overview](#Overview) section or learn more about [Pravega](https://pravega.io/), [Flink](https://flink.apache.org/), and [Pravega Flink Connectors](https://github.com/pravega/flink-connectors) for their usage and appropriate scenarios.

## Overview

* Pravega
  > Pravega is an open-source distributed storage service implementing Streams. It offers Stream as the main primitive for the foundation of reliable storage systems: a high-performance, durable, elastic, and unlimited append-only byte stream with strict ordering and consistency.
* PyFlink
  > PyFlink is a Python API for Apache Flink that allows you to build scalable batch and streaming workloads, such as real-time data processing pipelines, large-scale exploratory data analysis, Machine Learning (ML) pipelines, and ETL processes.
* Pravega Flink Connectors
  > The connectors can be used to build end-to-end stream processing pipelines that use Pravega as the stream storage and message bus, and Apache Flink for computation over the streams.

**TL;DR** With the help of the connectors, your streaming workload can safely rely on Pravega as the streaming storage which provides **end-to-end exactly-once processing pipelines** and **seamless integration with Flink's checkpoints and savepoints mechanism** with the help of connectors.

This Python API of connectors provides both **`PyFlink Table API`** and **`PyFlink DataStream API`**. We will cover them respectively at [Table API](#Table-API) and [DataStream API](#DataStream-API).

## Pre-requisites

To get everything ready, all the following conditions should meet.

1. Pravega running. See [here](http://pravega.io/docs/latest/getting-started/) for instructions.
2. Python and its packages installed. See [PyFlink setup](https://ci.apache.org/projects/flink/flink-docs-stable/docs/dev/python/installation/).
3. Latest Pravega Flink connectors at [the release page](https://github.com/pravega/flink-connectors/releases).
4. Apache Flink running.

> If you wish to use Anaconda instead, replace `pip` with `conda` in the steps above.

## Technical Details

We postpone the actual tutorials here is because understanding the underlying `PyFlink` mechanism is crucial for using connectors' Python API.

🚨 **Python wrapper** is the key for this section and keeps it in mind all the time.

Everything you see in the Python API is a wrapper that points to an actual Java object in the JVM. So basically, the Python API does nothing but simply calls the corresponding Java API for you.

If you wish to have other unimplemented things such as a particular stream cut or event router, you could refer to `DefaultCredentials` implementation in the Python API. (For `interface` implementation, see [Implementing Java interfaces from Python (callback)](https://www.py4j.org/advanced_topics.html#implementing-java-interfaces-from-python-callback))

Consider a simple Java class (`DefaultCredentials`):

```java
package io.pravega.shared.security.auth;

// omit imports

public class DefaultCredentials implements Credentials {
    private final String token;

    public DefaultCredentials(@NonNull String password, @NonNull String userName) {
        String decoded = userName + ":" + password;
        this.token = Base64.getEncoder().encodeToString(decoded.getBytes(StandardCharsets.UTF_8));
    }

    // omit other members and methods
}
```

To use this in Python, simply do the following:

```python
from py4j.java_gateway import JavaObject
from pyflink.java_gateway import get_gateway

username, password = 'admin', '1111_aaaa'

# create the java object
j_default_credentials: JavaObject = get_gateway() \
    .jvm.io.pravega.shared.security.auth \
    .DefaultCredentials(username, password)

# and when you want to use this
j_pravega_config: JavaObject = get_gateway() \
            .jvm.io.pravega.connectors.flink \
            .PravegaConfig.fromDefaults() \
            .withCredentials(j_default_credentials)
```

🚨 The magic here is that you could call any java method with matching parameters. If the parameter is a primitive type like `Integer` and `String`, you could simply pass the corresponding Python type like `int` and `str`. For derived types like `DefaultCredentials`, calling the constructor and pass the corresponding `JavaObject` is sufficient.

## Table API

✈ Table API is suitable for pure SQL use. See [Flink Pravega Table API samples for Python](https://github.com/pravega/pravega-samples/blob/dev/scenarios/pravega-flink-connector-sql-samples/python.md) for more details.

## DataStream API

DataStream programs in Flink are regular programs that implement transformations on data streams (e.g., filtering, updating state, defining windows, aggregating). The data streams are initially created from various sources (e.g., message queues, socket streams, files). Results are returned via sinks, which may for example write the data to files, or to standard output (for example the command line terminal).

We follow the official [Intro to the Python DataStream API](https://ci.apache.org/projects/flink/flink-docs-master/docs/dev/python/datastream/intro_to_datastream_api/) documentation. For topics we do not cover below such as the Table & SQL conversion-related things, feel free to visit in need.

### Common Structure of Python DataStream API Programs with Pravega Flink Connectors

```python
from pyflink.common import WatermarkStrategy, Row
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import NumberSequenceSource
from pyflink.datastream.functions import RuntimeContext, MapFunction
from pyflink.datastream.state import ValueStateDescriptor

from pravega_config import PravegaConfig
from pravega_writer import FlinkPravegaWriter

CONTROLLER_URI = 'tcp://localhost:9090'
SCOPE = 'scope'
STREAM = 'stream'


class MyMapFunction(MapFunction):
    def open(self, runtime_context: RuntimeContext):
        state_desc = ValueStateDescriptor('cnt', Types.PICKLED_BYTE_ARRAY())
        self.cnt_state = runtime_context.get_state(state_desc)

    def map(self, value):
        cnt = self.cnt_state.value()
        if cnt is None or cnt < 2:
            self.cnt_state.update(1 if cnt is None else cnt + 1)
            return value[0], value[1] + 1
        else:
            return value[0], value[1]


def state_access_demo():
    # 1. create a StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()

    # 2. create source DataStream
    seq_num_source = NumberSequenceSource(1, 10000)
    ds = env.from_source(
        source=seq_num_source,
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name='seq_num_source',
        type_info=Types.LONG())

    # 3. define the execution logic
    ds = ds.map(lambda a: Row(a % 4, 1), output_type=Types.ROW([Types.LONG(), Types.LONG()])) \
           .key_by(lambda a: a[0]) \
           .map(MyMapFunction(), output_type=Types.ROW([Types.LONG(), Types.LONG()]))

    # 4. create sink and emit result to sink
    pravega_config = PravegaConfig(uri=CONTROLLER_URI, scope=SCOPE)
    pravega_writer = FlinkPravegaWriter(
        stream=STREAM,
        pravega_config=pravega_config,
        serialization_schema=SimpleStringSchema())
    ds.add_sink(pravega_writer)

    # 5. execute the job
    env.execute('state_access_demo')


if __name__ == '__main__':
    state_access_demo()

```

### Create a StreamExecutionEnvironment

The StreamExecutionEnvironment is a central concept of the DataStream API program. The following code example shows how to create a StreamExecutionEnvironment:

```python
from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()

# for EXACTLY_ONCE writer mode, checkpoint is required
env.enable_checkpointing(1000, CheckpointingMode.EXACTLY_ONCE)

# add connectors jar if the job is not submitted with --jarfile options
env.add_jars("file:///path/to/pravega-connectors-flink.jar")
```

### Create a DataStream

The DataStream API gets its name from the special DataStream class that is used to represent a collection of data in a Flink program. You can think of them as immutable collections of data that can contain duplicates. This data can either be finite or unbounded, the API that you use to work on them is the same.

A `DataStream` is similar to a regular Python `Collection` in terms of usage but is quite different in some key ways. They are immutable, meaning that once they are created you cannot add or remove elements. You can also not simply inspect the elements inside but only work on them using the `DataStream` API operations, which are also called transformations.

You can create an initial `DataStream` by adding a source in a Flink program. Then you can derive new streams from this and combine them by using API methods such as `map`, `filter`, and so on.

#### Create using Pravega Flink Connectors

`Datastream` can be created from a list object like [this official example](https://ci.apache.org/projects/flink/flink-docs-stable/docs/dev/python/datastream/intro_to_datastream_api/#create-from-a-list-object). But here we will focus on how to read from external source such as Pravega. This is achieved by using DataStream connectors with method `add_source` as following:

```python
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment

from pravega_config import PravegaConfig
from pravega_reader import FlinkPravegaReader

CONTROLLER_URI = 'tcp://localhost:9090'
SCOPE = 'scope'
STREAM = 'stream'

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
env.add_jars("file:///path/to/pravega-connectors-flink.jar")

pravega_config = PravegaConfig('tcp://localhost:9090', SCOPE)
pravega_reader = FlinkPravegaReader(
    stream=STREAM,
    pravega_config=pravega_config,
    deserialization_schema=SimpleStringSchema(),
    enable_metrics=False)

ds = env.add_source(pravega_reader)
ds.print()

env.execute("reader_example")
```

*Unified Source through `from_source` is not supported yet.*

### DataStream Transformations

Operators transform one or more `DataStream` into a new `DataStream`. Programs can combine multiple transformations into sophisticated dataflow topologies.

The following example shows a simple example about how to convert a `DataStream` into another `DataStream` using map transformation:

```python
ds = ds.map(lambda a: a + 1)
```

See [operators](https://ci.apache.org/projects/flink/flink-docs-stable/docs/dev/python/datastream/operators/overview/) for an overview of the available `DataStream` transformations.

### Emit Results

#### Print

You can call the `print` method to print the data of a `DataStream` to the standard output:

```python
ds.print()
```

#### Collect results to client

You can call the `execute_and_collect` method to collect the data of a `DataStream` to client:

```python
with ds.execute_and_collect() as results:
    for result in results:
        print(result)
```

#### Emit results to Pravega via Pravega Flink Connectors

You can call the `add_sink` method to emit the data of a `DataStream` to a DataStream sink connector:

```python
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode

from pravega_config import PravegaConfig
from pravega_writer import FlinkPravegaWriter, PravegaWriterMode

CONTROLLER_URI = 'tcp://localhost:9090'
SCOPE = 'scope'
STREAM = 'stream'

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
env.enable_checkpointing(1000, CheckpointingMode.EXACTLY_ONCE)
env.add_jars("file:///path/to/pravega-connectors-flink.jar")

pravega_config = PravegaConfig(uri=CONTROLLER_URI, scope=SCOPE)
pravega_writer = FlinkPravegaWriter(stream=STREAM,
                                    pravega_config=pravega_config,
                                    writer_mode=PravegaWriterMode.ATLEAST_ONCE,
                                    serialization_schema=SimpleStringSchema())

ds = env.from_collection(collection=['a', 'bb', 'ccc'],
                         type_info=Types.STRING())
ds.add_sink(pravega_writer)

env.execute("writer_example")
```

*Unified Sink through `sink_to` is not supported yet.*

### Submit Job

Finally, you should call the `StreamExecutionEnvironment.execute` method to submit the DataStream API job for execution:

```python
env.execute()
```

Or submit via `flink` command line interface:

```bash
flink run --python ./pravega_examples.py --pyFiles ./pravega_config.py --pyFiles ./pravega_writer.py --jarfile /path/to/pravega-connectors-flink.jar
```
