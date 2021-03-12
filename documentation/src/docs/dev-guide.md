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
* Apache Flink running(Check [Starting-Flink](#Starting-Flink) to run Apache Flink)
* Use Gradle or Maven

# Compatibility Matrix

The [master](https://github.com/pravega/flink-connectors) branch will always have the most recent
supported versions of Flink and Pravega.

| Git Branch | Pravega Version | Java Version To Build Connector | Java Version To Run Connector | Flink Version                                                                        |
|----------|-----------------|---------------------------------|-------------------------------|-----------------------------------------------------------------------------------|
|    [master](https://github.com/pravega/flink-connectors)       | 0.10             | Java 11                         | Java 8 or 11                  |               1.11.2               |
| [r0.10-flink1.11](https://github.com/pravega/flink-connectors/tree/r0.10-flink1.11)            | 0.10             | Java 11                          | Java 8 or 11                         |  1.11.2  |
| [r0.10-flink1.10](https://github.com/pravega/flink-connectors/tree/r0.10-flink1.10)          | 0.10             | Java 11                          | Java 8 or 11                       |  1.10.3  |
|    [r0.9](https://github.com/pravega/flink-connectors/tree/r0.9)        | 0.9             | Java 11                         | Java 8 or 11                       |  1.11.2  |
|  [r0.9-flink1.10](https://github.com/pravega/flink-connectors/tree/r0.9-flink1.10)          | 0.9             | Java 11                         |Java 8 or 11                        |   1.10.3 |
| [r0.9-flink1.9](https://github.com/pravega/flink-connectors/tree/r0.9-flink1.9)           | 0.9             | Java 11                         | Java 8 or 11                       |  1.9.0  |



# Goal
In this guide, we will create a straightforward example application that writes data collected from an external network stream into a Pravega Stream and read the data from the Pravega Stream.
We recommend that you follow the instructions from [Bootstrapping project](#Bootstrapping-the-Project) onwards to create the application step by step.
However, you can go straight to the completed example at [flink-connector-examples](https://github.com/pravega/pravega-samples/tree/master/flink-connector-examples).




# Starting Flink
Download Flink release and un-tar it.
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
```
 gradle init --type java-application
```
Add the below snippet to dependencies section of build.gradle in the app directory
```
// https://mvnrepository.com/artifact/org.apache.flink/flink-streaming-java
compileOnly group: 'org.apache.flink', name: 'flink-streaming-java_2.12', version: '1.11.3'

// https://mvnrepository.com/artifact/io.pravega/pravega-connectors-flink
implementation group: 'io.pravega', name: 'pravega-connectors-flink_2.12', version: '0.5.1'
```
Invoke `gradle run` to run the project.

 
<details>
<summary>Expected output</summary>
<p>

```
$ gradle run

> Task :app:run
Hello World!

BUILD SUCCESSFUL in 890ms
2 actionable tasks: 2 executed

```

</p>
</details>

### Maven
Add below dependencies into Maven POM
```
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-java_2.11</artifactId>
  <version>1.12.0</version>
  <scope>provided</scope>
</dependency>

<dependency>
  <groupId>io.pravega</groupId>
  <artifactId>pravega-connectors-flink-1.9_2.12</artifactId>
  <version>0.6.0</version>
</dependency>
```
You can also check [maven-quickstart](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/project-configuration.html#maven-quickstart) to find more to start with Maven


## Create a Word Count Writer

Let’s first create a pravega configuration reading from arguments:
```java
ParameterTool params = ParameterTool.fromArgs(args);
PravegaConfig pravegaConfig = PravegaConfig
        .fromParams(params)
        .withDefaultScope(Constants.DEFAULT_SCOPE);
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
        .withEventRouter(new EventRouter())
        .withSerializationSchema(PravegaSerialization.serializationFor(String.class))
        .build();
dataStream.addSink(writer).name("Pravega Stream");
```
Then we execute the job within the Flink environment
```java
env.execute("WordCountWriter");
```
Executing the above lines should ensure we have created a WordCountWriter job

## Create a Word Count Reader
Creating a Word Count Reader is similar to Word Count Writer
First create a pravega configuration reading from arguments:
```java
ParameterTool params = ParameterTool.fromArgs(args);
PravegaConfig pravegaConfig = PravegaConfig
        .fromParams(params)
        .withDefaultScope(Constants.DEFAULT_SCOPE);
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
        .withDeserializationSchema(PravegaSerialization.deserializationFor(String.class))
        .build();
```
Then create a datastream count each word over a 10 second time period
```java
DataStream<WordCount> dataStream = env.addSource(source).name("Pravega Stream")
        .flatMap(new WordCountReader.Splitter())
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
env.execute("WordCountReader");
```

## Run in flink environment
First build your application. From Flink's perspective, the connector to Pravega is part of the streaming application (not part of Flink's core runtime), so the connector code must be part of the application's code artifact (JAR file). Typically, a Flink application is bundled as a `fat-jar` (also known as an `uber-jar`) , such that all its dependencies are embedded.

Use gradle or maven to assemble a distribution folder containing the Flink programs as a ready-to-deploy fat-jar
```
./gradlew clean installDist

ls -R .../build/install/${yourapp}
bin	lib

.../build/install/${yourapp}/bin:
${yourapp} ...

../build/install/${yourapp}/lib:
${yourapp}.jar ...
```

Make sure your Pravega and Flink are running. Then start a local server using `netcat`:
```
$ nc -lk 9999
```

Start WordCountWriter Flink job
```
cd .../build/install/${yourapp}/lib
flink run -c <classname> ${yourapp}.jar --host localhost --port 9999 --controller tcp://localhost:9090
```
Start WordCountReader Flink job
```
cd .../build/install/${yourapp}/lib
flink run -c <classname> ${yourapp}.jar --controller tcp://localhost:9090
```
Enter some text in the windows where `netcat` is running:
```
aa bb cc aa
```
Then you can check output either on your browser `http://<your_flink_host>:8081` or check the flink log on `.../flink/log/`:
```
Word: aa:  Count: 2
Word: cc:  Count: 1
Word: bb:  Count: 1
```

# What’s next?
This guide covered the creation of a application that uses Flink connector to read and wirte from a pravega stream. However, there is much more. We recommend continuing the journey by going through [flink connector documents](https://pravega.io/docs/latest/connectors/flink-connector/) and check other examples on [flink-connector-examples](https://github.com/pravega/pravega-samples/tree/master/flink-connector-examples).
