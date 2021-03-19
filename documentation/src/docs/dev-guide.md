<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Flink Connector - Dev Guide
Learn how to build your own applications that using Flink connector for Pravega.

 
# Prerequisites
To complete this guide, you need:

* JDK 8 or 11 installed with JAVA_HOME configured appropriately
* Pravega running(Check [here](https://pravega.io/docs/latest/getting-started/) to get started with Pravega)
* Use Gradle or Maven



# Goal
In this guide, we will create a straightforward example application that writes data collected from an external network stream into a Pravega Stream and read the data from the Pravega Stream.
We recommend that you follow the instructions from [Bootstrapping project](#Bootstrapping-the-Project) onwards to create the application step by step.
However, you can go straight to the completed example at [flink-connector-examples](https://github.com/pravega/pravega-samples/tree/master/flink-connector-examples).




# Starting Flink
Download Flink release and un-tar it. We use Flink 1.11.2 here. 
```
$ tar -xzf flink-1.11.2-bin-scala_2.11.tgz
$ cd flink-1.11.2-bin-scala_2.11
```
Start a cluster
```
$ ./bin/start-cluster.sh
Starting cluster.
Starting standalonesession daemon on host.
Starting taskexecutor daemon on host.
```
When you are finished you can quickly stop the cluster and all running components.
```
$ ./bin/stop-cluster.sh
```

# Bootstrapping the Project.

Using Gradle or Maven to bootstrap a sample application against Pravega. Let's create a word count application as an example.
### Gradle
You can follow [here](https://ci.apache.org/projects/flink/flink-docs-stable/dev/project-configuration.html#gradle) to create a gradle project.

Add the below snippet to dependencies section of build.gradle in the app directory, connector dependencies should be part of the shadow jar. For flink connector dependency, we need to choose the connector which aligns the Flink major version and Scala version if you use Scala, along with the same Pravega version you run.
```
compile group 'org.apache.flink', name: 'flink-streaming-java_2.12', version: '1.11.2'

flinkShadowJar group: 'io.pravega', name: 'pravega-connectors-flink-1.11_2.12', version: '0.9.0'
```
Define custom configurations `flinkShadowJar`
```
// -> Explicitly define the // libraries we want to be included in the "flinkShadowJar" configuration!
configurations {
    flinkShadowJar // dependencies which go into the shadowJar

    // always exclude these (also from transitive dependencies) since they are provided by Flink
    flinkShadowJar.exclude group: 'org.apache.flink', module: 'force-shading'
    flinkShadowJar.exclude group: 'com.google.code.findbugs', module: 'jsr305'
    flinkShadowJar.exclude group: 'org.slf4j'
    flinkShadowJar.exclude group: 'org.apache.logging.log4j'
}
```

Invoke `gradle clean shadowJar` to build/package the project. You will find a JAR file that contains your application, plus connectors and libraries that you may have added as dependencies to the application: `build/libs/<project-name>-<version>-all.jar`.


### Maven

You can check [maven-quickstart](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/project-configuration.html#maven-quickstart) to find how to start with Maven.

Add below dependencies into Maven POM, these dependencies should be part of the shadow jar
```
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-java_2.12</artifactId>
  <version>1.11.2</version>
  <scope>provided</scope>
</dependency>

<dependency>
  <groupId>io.pravega</groupId>
  <artifactId>pravega-connectors-flink-1.11_2.12</artifactId>
  <version>0.9.0</version>
</dependency>
```

Invoke `mvn clean package` to build/package your project. You will find a JAR file that contains your application, plus connectors and libraries that you may have added as dependencies to the application: `target/<artifact-id>-<version>.jar`.




## Create an application that writes to Pravega

Let’s first create a pravega configuration reading from arguments:
```java
ParameterTool params = ParameterTool.fromArgs(args);
PravegaConfig pravegaConfig = PravegaConfig
        .fromParams(params)
        .withDefaultScope("my_scope");
```
Then we need to initialize the Flink execution environment
```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
```
Create a datastream that gets input data by connecting to the socket
```java
DataStream<String> dataStream = env.socketTextStream(host, port);
```
A Pravega Stream may be used as a data sink within a Flink program using an instance of `io.pravega.connectors.flink.FlinkPravegaWriter`. We add an instance of the writer to the dataflow program:
```java
FlinkPravegaWriter<String> writer = FlinkPravegaWriter.<String>builder()
        .withPravegaConfig(pravegaConfig)
        .forStream(stream)
        .withSerializationSchema(new SimpleStringSchema())
        .build();
dataStream.addSink(writer).name("Pravega Sink");
```
Then we execute the job within the Flink environment
```java
env.execute("PravegaWriter");
```
Executing the above lines should ensure we have created a PravegaWriter job

## Create an application that reads from Pravega
Creating a Pravega Reader is similar to Pravega Writer
First create a pravega configuration reading from arguments:
```java
ParameterTool params = ParameterTool.fromArgs(args);
PravegaConfig pravegaConfig = PravegaConfig
        .fromParams(params)
        .withDefaultScope("my_scope");
```
Initialize the Flink execution environment
```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
```
A Pravega Stream may be used as a data source within a Flink streaming program using an instance of `io.pravega.connectors.flink.FlinkPravegaReader`. The reader reads a given Pravega Stream (or multiple streams) as a DataStream
```java
FlinkPravegaReader<String> source = FlinkPravegaReader.<String>builder()
        .withPravegaConfig(pravegaConfig)
        .forStream(stream)
        .withDeserializationSchema(new SimpleStringSchema())
        .build();
```
Then create a datastream count each word over a 10 second time period
```java
DataStream<WordCount> dataStream = env.addSource(source).name("Pravega Stream")
        .flatMap(new Tokenizer()) // The Tokenizer() splits the line into words, and emit streams of "WordCount(word, 1)"
        .keyBy("word")
        .timeWindow(Time.seconds(10))
        .sum("count");
```
Create an output sink to print to stdout for verification
```java
dataStream.print();
```
Then we execute the job within the Flink environment
```java
env.execute("PravegaReader");
```

## Run in flink environment
First build your application. From Flink's perspective, the connector to Pravega is part of the streaming application (not part of Flink's core runtime), so the connector code must be part of the application's code artifact (JAR file). Typically, a Flink application is bundled as a `fat-jar` (also known as an `uber-jar`) , such that all its dependencies are embedded.

Make sure your Pravega and Flink are running. Use the packaged jar, and run:
```
flink run -c <classname> ${your-app}.jar --controller <pravega-controller-uri>
```

# What’s next?
This guide covered the creation of a application that uses Flink connector to read and wirte from a pravega stream. However, there is much more. We recommend continuing the journey by going through [flink connector documents](https://pravega.io/docs/latest/connectors/flink-connector/) and check other examples on [flink-connector-examples](https://github.com/pravega/pravega-samples/tree/master/flink-connector-examples).
