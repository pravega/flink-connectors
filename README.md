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
$ ./gradlew build
...
BUILD SUCCESSFUL
```

