<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Serialization

**Serialization** refers to converting a data element in your Flink program to/from a message in a Pravega stream.

Flink defines a standard interface for data serialization to/from byte messages delivered by various connectors. The core interfaces are:
- [`org.apache.flink.streaming.util.serialization.SerializationSchema`]( https://ci.apache.org/projects/flink/flink-docs-stable/api/java/org/apache/flink/streaming/util/serialization/SerializationSchema.html)
- [`org.apache.flink.streaming.util.serialization.DeserializationSchema`]( https://ci.apache.org/projects/flink/flink-docs-stable/api/java/org/apache/flink/streaming/util/serialization/DeserializationSchema.html)

Built-in serializers include:
- [`org.apache.flink.streaming.util.serialization.SimpleStringSchema`](https://ci.apache.org/projects/flink/flink-docs-stable/api/java/org/apache/flink/streaming/util/serialization/SimpleStringSchema.html)
- [`org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema`](https://ci.apache.org/projects/flink/flink-docs-stable/api/java/org/apache/flink/streaming/util/serialization/TypeInformationSerializationSchema.html)

The Pravega connector is designed to use Flink's serialization interfaces. For example, to read each stream event as a UTF-8 string:
```java
DeserializationSchema<String> schema = new SimpleStringSchema();
FlinkPravegaReader<String> reader = new FlinkPravegaReader<>(..., schema);
DataStream<MyEvent> stream = env.addSource(reader);
```

## Interoperability with Other Applications
A common scenario is using Flink to process Pravega stream data produced by a non-Flink application. The Pravega client library used by such applications defines the [`io.pravega.client.stream.Serializer`](http://pravega.io/docs/latest/javadoc/clients/io/pravega/client/stream/Serializer.html) interface for working with event data. The implementations of `Serializer` directly in a Flink program via built-in adapters can be used:
- [`PravegaSerializationSchema`](https://github.com/pravega/flink-connectors/blob/master/src/main/java/io/pravega/connectors/flink/serialization/PravegaSerializationSchema.java)
- [`PravegaDeserializationSchema`](https://github.com/pravega/flink-connectors/blob/master/src/main/java/io/pravega/connectors/flink/serialization/PravegaDeserializationSchema.java)

Below is an example, to pass an instance of the appropriate Pravega de/serializer class to the adapter's constructor:
```java
import io.pravega.client.stream.impl.JavaSerializer;
...
DeserializationSchema<MyEvent> adapter = new PravegaDeserializationSchema<>(
    MyEvent.class, new JavaSerializer<MyEvent>());
FlinkPravegaReader<MyEvent> reader = new FlinkPravegaReader<>(..., adapter);
DataStream<MyEvent> stream = env.addSource(reader);
```  

Note that the Pravega serializer must implement `java.io.Serializable` to be usable in a Flink program.
