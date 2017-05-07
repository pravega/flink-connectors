# Pravega Flink Connectors

Connectors for Pravega to be used with Flink jobs.

## Getting Started
### Building Pravega

Install the Pravega client libraries to your local Maven repository.
```
$ git clone https://github.com/pravega/pravega.git
$./gradlew publishMavenPublicationToMavenLocal
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
$ ./gradlew publishShadowPublicationToMavenLocal
```

**Note:** This connector is built against flink version 1.3-SNAPSHOT

## Pravega Source
Instantiate a FlinkPravegaReader instance and add it as a source to the Flink streaming job. 
The source does a tail read of the supplied set of streams.

```
// Set startTime to 0 to read from the beginning of the streams
FlinkPravegaReader<EventType> pravegaSource = new FlinkPravegaReader<>(
        "tcp://localhost:9090",
        scopeName,
        listOfStreams,
        startTime,
        new AbstractDeserializationSchema<EventType>());
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
```
DataStreamSource<EventType> dataStream = ...
...
FlinkPravegaWriter<EventType> pravegaSink = new FlinkPravegaWriter<>(
        "tcp://localhost:9090", 
        scopeName, 
        streamName, 
        new SerializationSchema(), 
        new PravegaEventRouter())
dataStream.addSink(pravegaSink);
```
