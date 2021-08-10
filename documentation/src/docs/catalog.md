<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->

# Pravega Catalogs

## General Catalog Introduction

[Flink Catalogs](https://ci.apache.org/projects/flink/flink-docs-stable/docs/dev/table/catalogs/) provide metadata such as databases, tables, partitions, views, functions and information needed to access data stored in a database or other external systems. It provides a unified API for managing metadata and making it accessible from the Table API and SQL Queries.

A Catalog enables users to reference existing metadata in their data systems and automatically maps them to Flink's corresponding metadata. For example, Flink can map JDBC tables to Flink tables automatically and users don’t have to manually re-writing DDLs in Flink. A catalog simplifies steps required to get started with connecting Flink and user's existing systems, improving the user experience.

## Basic ideas and building blocks of Pravega Catalog

Pravega uses terms such as *streams* and *scopes* for managing streaming data, but it does not have the concepts of tables and databases. These terms however can be thought of as similar. For example, if a Pravega stream contains semi-structured data such as JSON format, it is feasible to map Pravega streams to Flink tables with the help of a schema registry service. 

[Pravega Schema Registry](https://github.com/pravega/schema-registry) is built for such a purpose. It is the registry service built on Pravega that helps store and manage schemas for data stored in Pravega streams. It also provides a factory of methods to standardize the serialization with built-in support for popular serialization formats in Avro, Protobuf, JSON schemas, as well as custom serialization.

### `PravegaRegistryFormatFactory`

When using Schema Registry serialization, further information is required in order to describe how to map binary data onto table columns.  A new table format named `pravega-registry` has been added to define this mapping.

**Note:** The `pravega-registry` format factory should ONLY be used with the `PravegaCatalog`. Currently it supports only Json and Avro formats without any additional encryption and compression codecs.

It has following options:

| Option                      | Required            | Default       | Type         | Description                                                                         |
|-----------------------------|---------------------|---------------|--------------|-------------------------------------------------------------------------------------|
| format                      | required            | (none)        | String       | Specify what format to use, here should be 'pravega-registry'                       |
| pravega-registry.uri        | required            | (none)        | String       | Pravega Schema Registry service URI                                                 |
| pravega-registry.namespace  | required            | (none)        | String       | Pravega Schema Registry namespace, should be the same name as Pravega scope         |
| pravega-registry.group-id   | required            | (none)        | String       | Pravega Schema Registry group ID, should be the same name as Pravega stream         |
| pravega-registry.format     | optional            | Avro          | String       | Default format for serialization in table sink, Valid values are 'Json' and 'Avro'  |
| pravega-registry.json.*     | optional            | (none)        | -            | Specification for json format, completely inherited from official Flink Json format factory, refer to this [doc](https://ci.apache.org/projects/flink/flink-docs-stable/docs/connectors/table/formats/json/#format-options) for details                                  |

A `PravegaCatalog` is built to manage Pravega streams as Flink tables based on it's schema registry and this table format.
It can map all the streams with its Json/Avro schema registered and users can directly read from/write to the stream without establishing connection with extra SQL DDL.

## Pravega as a Catalog

The `PravegaCatalog` enables users to connect Flink to Pravega streams. The following table shows the mapping between Flink Catalog and Pravega terms:

| Flink Catalog terms                  | Pravega terms     |
|--------------------------------------|-------------------|
| catalog name (defined in Flink only) | N/A               |
| database name                        | scope name        |
| table name                           | stream name       |


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

### Catalog options
Pravega Catalog supports the following options:

- name: required, name of the catalog
- type: required, type of the catalog, should be 'pravega' here
- controller-uri: required, URI of the Pravega controller connected to
- schema-registry-uri: required, URI of the Schema Registry service connected to
- default-database: required, default Pravega scope which must be created already
- serialization.format: optional, a static serialization format for the catalog, valid values are 'Avro'(default) and 'Json', this is the format used for all the table sinks in the catalog.
- json.*: optional, json format specifications for the catalog table sink, will inherit into `PravegaRegistryFormatFactory` for all catalog table sinks

## How to use Pravega Catalog 

Users can use SQL DDL or Java/Scala programatically to create and register Pravega Flink Catalog.

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

#### YAML
```yaml
execution:
  ...
  current-catalog: pravega_catalog  # set the PravegaCatalog as the current catalog of the session
  current-database: scope1
 
catalogs:
  - name: pravega_catalog
    type: pravega
    controller-uri: tcp://localhost:9090
    schema-registry-uri: http://localhost:9092
    default-database: scope1
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

## Useful Flink links
See [Flink Table catalogs docs](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/catalogs.html) for more information on the general Catalog concepts and more detailed operations.