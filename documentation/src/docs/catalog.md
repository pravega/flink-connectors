<!--
Copyright Pravega Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Pravega Catalogs

Pravega Catalog allows for accessing Pravega streams with Flink Table API and SQL Queries.

## Dependencies

In order to use the Pravega Catalog, the following dependencies are required for both projects using a build automation tool (such as Maven or SBT) 
and SQL Client with SQL JAR bundles.

### Maven dependency
```xml
<dependency>
  <groupId>io.pravega</groupId>
  <artifactId>pravega-connectors-flink-{flinkVersion}_{flinkScalaVersion}</artifactId>
  <version>{pravegaVersion}</version>
</dependency>

<dependency>
  <groupId>io.pravega</groupId>
  <artifactId>schemaregistry-serializers</artifactId>
  <version>{schemaRegistryVersion}</version>
  <classifier>all</classifier>
</dependency>
```

> **Note:** The default serialization format for catalog is Avro, if you are using default Avro serialization format, you need to include
>  the following dependency for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.
>```xml
><dependency>
>  <groupId>org.apache.flink</groupId>
>  <artifactId>flink-avro</artifactId>
>  <version>{flinkVersion}</version>
></dependency>
>```

## General Catalog Introduction

[Flink Catalogs](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/catalogs/) provide metadata such as databases, tables, partitions, views, functions and information needed to access data stored in a database or other external systems. It provides a unified API for managing metadata and making it accessible from the Table API and SQL Queries.

A Catalog enables users to reference existing metadata in their data systems and automatically maps them to Flink's corresponding metadata. For example, Flink can map JDBC tables to Flink tables automatically and users don’t have to manually re-writing DDLs in Flink. A catalog simplifies steps required to get started with connecting Flink and user's existing systems, improving the user experience.

## Basic ideas and building blocks of Pravega Catalog

Pravega uses terms such as *streams* and *scopes* for managing streaming data, but it does not have the concepts of tables and databases. These terms however can be thought of as similar. For example, if a Pravega stream contains semi-structured data such as JSON format, we can translate Pravega stream into a Flink table by utilizing Pravega Schema Registry service.

[Pravega Schema Registry](https://github.com/pravega/schema-registry) is built for such a purpose. It is the registry service built on Pravega that helps store and manage schemas for data stored in Pravega streams. It also provides a factory of methods to standardize the serialization with built-in support for popular serialization formats in Avro, Protobuf, JSON schemas, as well as custom serialization.

When using Schema Registry serialization, further information is required in order to describe how to map binary data onto table columns. We have introduced an internal format factory `PravegaRegistryFormatFactory`. Currently it supports Json and Avro formats without any additional encryption and compression codecs.

With the help of Schema Registry service, it is feasible to map Pravega streams to Flink tables as the following table shows:

| Flink Catalog terms                  | Pravega terms |
|--------------------------------------|---------------|
| database                             | scope         |
| table                                | stream        |

With such mapping we don't need to rewrite DDLs to create table or manually deal with many connection parameters to create tables. It lets us clearly separate making the data available from consuming it. That separation improves productivity, security, and compliance when working with data.

## Catalog options

| Option                     | Required | Default | Type    | Description                                                                                                                                                                                                                        |
|----------------------------|----------|---------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| type                       | required | (none)  | String  | Type of the catalog, should be 'pravega' here                                                                                                                                                                                      |
| controller-uri             | required | (none)  | String  | URI of the Pravega controller connected to                                                                                                                                                                                         |
| schema-registry-uri        | required | (none)  | String  | URI of the Schema Registry service connected to                                                                                                                                                                                    |
| default-database           | required | (none)  | String  | Default catalog database, which is mapped to Pravega scope, should be created already                                                                                                                                              |
| serialization.format       | optional | Avro    | String  | A static serialization format for the catalog, valid values are 'Avro'(default) and 'Json', this is the format used for all the table sinks in the catalog                                                                         |
| security.auth-type         | optional | (none)  | String  | The static authentication/authorization type for security for Pravega                                                                                                                                                              |
| security.auth-token        | optional | (none)  | String  | Static authentication/authorization token for security for Pravega                                                                                                                                                                 |
| security.validate-hostname | optional | true    | Boolean | Flag to decide whether to enable host name validation when TLS is enabled for Pravega                                                                                                                                              |
| security.trust-store       | optional | (none)  | String  | Trust store for Pravega client                                                                                                                                                                                                     |
| json.*                     | optional | (none)  | -       | Json format specifications, completely inherited from official Flink Json format factory, refer to this [doc](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/formats/json/#format-options) for details |

## How to use Pravega Catalog 

Users can use SQL DDL or Java/Scala programmatically to create and register Pravega Flink Catalog.

#### SQL

```sql
CREATE CATALOG pravega_catalog WITH(
    'type' = 'pravega',
    'default-database' = 'scope1',
    'controller-uri' = 'tcp://localhost:9090',
    'schema-registry-uri' = 'http://localhost:9092'
);

USE CATALOG pravega_catalog;
```

#### Java

```java
TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());

String name                 = "pravega_catalog";
String defaultDatabase      = "scope1";
String controllerURI        = "tcp://localhost:9090";
String schemaRegistryURI    = "http://localhost:9092";

PravegaCatalog catalog = new PravegaCatalog(name, defaultDatabase, controllerURI, schemaRegistryURI);
tableEnv.registerCatalog("pravega_catalog", catalog);

// set the PravegaCatalog  as the current catalog of the session
tableEnv.useCatalog("pravega_catalog");
```

After that, you can operate Pravega scopes and streams with SQL commands. Here are some examples.

```sql
-- List all the scopes
SHOW DATABASES;

-- Create a scope
CREATE DATABASE scope2 WITH (...);

-- Delete a scope, if 'CASCADE' is followed, all the streams will also be dropped.
DROP DATABASE scope2;

-- List all the streams with schema registered in the scope
SHOW TABLES;

-- Create a stream and register the schema
CREATE TABLE mytable (name STRING, age INT) WITH (...);

-- Delete a stream, the schema group will also be deleted 
DROP DATABASE scope2;

-- Scan/Query the stream `test_table` as a table
SELECT * FROM pravega_catalog.scope1.test_table;
SELECT count(DISTINCT name) FROM test_table;

-- Write rows/Copy a table into the stream `test_table`
INSERT INTO test_table VALUES('Tom', 30);
INSERT INTO test_table SELECT * FROM mytable;
```

Currently `PravegaCatalog` only supports a limited set of `Catalog` methods:

```java
// The supported methods by Pravega Catalog
PravegaCatalog.listDatabases();
PravegaCatalog.getDatabase(String databaseName);
PravegaCatalog.databaseExists(String databaseName);
PravegaCatalog.createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists);
PravegaCatalog.dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade);
PravegaCatalog.listTables(String databaseName);
PravegaCatalog.getTable(ObjectPath tablePath);
PravegaCatalog.tableExists(ObjectPath tablePath);
PravegaCatalog.dropTable(ObjectPath tablePath, boolean ignoreIfNotExists);
PravegaCatalog.createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists);
```

Only these database and table operations are currently supported. Views/partitions/functions/statistics operations are NOT supported in `PravegaCatalog`.

## Useful Flink links

See [Flink Table catalogs docs](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/catalogs/) for more information on the general Catalog concepts and more detailed operations.
