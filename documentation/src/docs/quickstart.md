<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->

# Getting Started
## Creating a Flink Stream Processing Project

**Note**: _You can skip this step if you have a streaming project set up already._

Please use the following project templates and setup guidelines, to set up a stream processing project with Apache Flink:

  - [Project template for Java](https://ci.apache.org/projects/flink/flink-docs-stable/quickstart/java_api_quickstart.html)
  - [Project template for Scala](https://ci.apache.org/projects/flink/flink-docs-release-1.6/quickstart/scala_api_quickstart.html)

Once after the set up, please follow the below instructions to add the **Flink Pravega connectors** to the project.

## Add the Connector Dependencies

To add the Pravega connector dependencies to your project, add the following entry to your project file: (For example, `pom.xml` for Maven)



```
<dependency>
  <groupId>io.pravega</groupId>
  <artifactId>pravega-connectors-flink_2.11</artifactId>
  <version>0.3.2</version>
</dependency>
```

Use appropriate version as necessary. The snapshot versions are published to [`jcenter`](https://oss.jfrog.org/artifactory/jfrog-dependencies/io/pravega/pravega-connectors-flink_2.11/) repository and the release artifacts are available in [`Maven Central`](https://mvnrepository.com/artifact/io.pravega/pravega-connectors-flink_2.11) repository.

Alternatively, we could build and publish the connector project to local maven repository by following the below steps and make use of that version as your application dependency.

```
./gradlew clean install
```

## Running / Deploying the Application

From Flink's perspective, the connector to Pravega is part of the streaming application (not part of Flink's core runtime), so the connector code must be part of the application's code artifact (JAR file). Typically, a Flink application is bundled as a _`fat-jar`_ (also known as an _`uber-jar`_) , such that all its dependencies are embedded.

 - The project set up should have been a success, if you have used the above linked [templates/guides](#creating-a-flink-stream-processing-project).

 - If you set up a application's project and dependencies manually, you need to make sure that it builds a _jar with dependencies_, to include both the application and the connector classes.
