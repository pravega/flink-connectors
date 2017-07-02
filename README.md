# Pravega Flink Connectors

Connectors to read and write [Pravega](http://pravega.io/) streams with [Apache Flink](http://flink.apache.org/) stream processing applications.

Build end-to-end stream processing pipelines that use Pravega as the stream storage and message bus, and Apache Flink for computation over the streams.


## Features & Highlights

  - **Exactly-once processing guarantees** for both reader and writer, supporting **end-to-end exactly-once processing pipelines**

  - Seamless integration with Flink's checkpoints & savepoints

  - Parallel readers and writers supporting high throughput and low latency processing


## Versions

The connectors require Apache Flink version **1.3** or newer.
The latest release of these connectors are linked against Flink release *1.3.1*, which should be compatible with all Flink *1.3.x* versions.


## Getting Started
### Building Pravega

Install the Pravega client libraries to your local Maven repository.
```
$ git clone https://github.com/pravega/pravega.git
$./gradlew install
```

### Building the Connector
Use the built-in gradle wrapper to build the connector.
```
$ git clone https://github.com/pravega/flink-connectors.git
$ ./gradlew clean build
...
BUILD SUCCESSFUL
```

### Creating the shaded connector jar
Use the following command to publish the shaded connector jar file. The jar file created is named as - pravega-connectors-flink_2.11-<version>.jar
```
$ ./gradlew install
```

## Pravega Source
Instantiate a FlinkPravegaReader instance and add it as a source to the Flink streaming job. 
The source does a tail read of the supplied set of streams.

```java
// Define your event deserializer 
AbstractDeserializationSchema<EventType> deserializer = ...

// Set startTime to 0 to read from the beginning of the streams
FlinkPravegaReader<EventType> pravegaSource = new FlinkPravegaReader<>(
        "tcp://localhost:9090",
        scopeName,
        listOfStreams,
        startTime,
        deserializer);
DataStreamSource<EventType> dataStream = flinkEnv
                                            .addSource(pravegaSource)
                                            .setParallelism(2)
                                            .enableCheckpointing(1000);
...

```

## Pravega Sink
We currently have 2 implementations of the sink. These will be merged into FlinkPravegaWriter soon.
* FlinkExactlyOncePravegaWriter
  * Should be only used with checkpointing enabled for providing exactly once guarantees using transactions.
* FlinkPravegaWriter
  * Should be used by flink jobs which have disabled checkpointing.
  
The usage is the same for both the connectors. Example:
```java
DataStreamSource<EventType> dataStream = ...
...
// Define Event Serializer.
SerializationSchema<EventType> serializer = ...

// Define the event router for selecting the keys to route events within pravega.
PravegaEventRouter router = ...

FlinkPravegaWriter<EventType> pravegaSink = new FlinkPravegaWriter<>(
        "tcp://localhost:9090", 
        scopeName, 
        streamName, 
        serializer, 
        router)
dataStream.addSink(pravegaSink);
```
