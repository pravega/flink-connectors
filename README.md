# pravega-connectors

Sample applications for Pravega.

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
$ ./gradlew install
```
