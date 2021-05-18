<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->

# Pravega Catalogs

Flink catalogs provide metadata, such as databases, tables, partitions, views, and functions and information needed to access data stored in a database or other external systems. It provide a unified API for managing metadata and making it accessible from the Table API and SQL Queries.

Catalog enables users to reference existing metadata in their data systems, and automatically maps them to Flink’s corresponding metadata. For Pravega, Flink can map Pravega streams as Flink table by treating Pravega scope as database and Pravega stream as table.

Pravega Schema Registry is the registry service that help store and manage schemas for the unstructured data stored in Pravega streams. The service has built in support for popular serialization formats in Avro, Profobuf and JSON schemas. With the help of Schema Registry, we can implement the `DeserializationFormatFactory` and `SerializationFormatFactory` interface which combines both Pravega Registry (De)Serializer and Flink Converter and, therefore, a byte array can be deserialized by Pravega Registry deserializer and then converted to Flink RowData that can be used in catalog, and vice versa. For serialization we only support Avro now since there are no convenient approaches to configure format prior to creating catalog tables;

The catalog factory defines a set of properties for configuring the catalog when the SQL CLI bootstraps. The set of properties will be passed to a discovery service where the service tries to match the properties to a CatalogFactory and initiate a corresponding catalog instance.

## How to use PravegaCatalog

The PravegaCatalog enables users to connect Flink to Pravega streams.

Currently, PravegaCatalog only support limited Catalog methods include:

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
Other Catalog methods are unsupported now.

### Usage of PravegaCatalog
Pravega Catalog supports the following options:

- name: required, name of the catalog
- type: required, type of the catalog
- controller-uri: required, uri of the Pravega controller connected to
- schema-registry-uri: required, uri of the Schema Registry service connected to
- default-database: required, default database to connect to

#### SQL
```sql
CREATE CATALOG pravega WITH(
    'type' = 'pravega',
    'default-database' = '...',
    'controller-uri' = '...',
    'schema-registry-uri' = '...'
);

USE CATALOG pravega;
```

#### Java
```java
EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
TableEnvironment tableEnv = TableEnvironment.create(settings);

String name                 = "pravega";
String defaultDatabase      = "mydb";
String controllerURI        = "...";
String schemaRegistryURI    = "...";

PravegaCatalog catalog = new PravegaCatalog(name, defaultDatabase, controllerURI, schemaRegistryURI);
tableEnv.registerCatalog("pravega", catalog);

// set the PravegaCatalog  as the current catalog of the session
tableEnv.useCatalog("pravega");
```

#### YAML
```yaml
execution:
  planner: blink
  type: streaming
  result-mode: table
 
catalogs:
  - name: pravega
    type: pravega
    controller-uri: "..."
    schema-registry-uri: "..."
    default-database: "mydb"
```

### Pravega Stream Mapping
Pravega doesn't have concepts like database or table. We need to map Pravega scopes and streams in order to use Pravega Catalog.
Therefore the metaspace mapping between Flink Catalog and Pravega is as following:

| Flink Catalog Metaspace Structure    | Pravega Structure |
|--------------------------------------|-------------------|
| catalog name (defined in Flink only) | N/A               |
| database name                        | scope name        |
| table name                           | stream name       |


## Catalog API
Only catalog program APIs are listed here. Users can achieve many of the same funtionalities with SQL DDL. For detailed DDL information, please refer to [SQL CREATE DDL](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/sql/create.html).

Only database and table operations are supported now, views/partitions/functions/statistics operations are NOT supported in PravegaCatalog

### Database operations
#### Java/Scala
```java
// create database
catalog.createDatabase("mydb", new CatalogDatabaseImpl(...), false);

// drop database
catalog.dropDatabase("mydb", false);

// get database
catalog.getDatabase("mydb");

// check if a database exist
catalog.databaseExists("mydb");

// list databases in a catalog
catalog.listDatabases();
```
#### Python
```
from pyflink.table.catalog import CatalogDatabase

# create database
catalog_database = CatalogDatabase.create_instance({"k1": "v1"}, None)
catalog.create_database("mydb", catalog_database, False)

# drop database
catalog.drop_database("mydb", False)

# alter database
catalog.alter_database("mydb", catalog_database, False)

# get database
catalog.get_database("mydb")

# check if a database exist
catalog.database_exists("mydb")

# list databases in a catalog
catalog.list_databases()
```
### Table operations
#### Java/Scala
```java
// create table
catalog.createTable(new ObjectPath("mydb", "mytable"), new CatalogTableImpl(...), false);

// drop table
catalog.dropTable(new ObjectPath("mydb", "mytable"), false);

// get table
catalog.getTable("mytable");

// check if a table exist or not
catalog.tableExists("mytable");

// list tables in a database
catalog.listTables("mydb");
```
#### Python
```
from pyflink.table import *
from pyflink.table.catalog import CatalogBaseTable, ObjectPath
from pyflink.table.descriptors import Kafka

table_schema = TableSchema.builder() \
    .field("name", DataTypes.STRING()) \
    .field("age", DataTypes.INT()) \
    .build()

table_properties = Kafka() \
    .version("0.11") \
    .start_from_earlist() \
    .to_properties()

catalog_table = CatalogBaseTable.create_table(schema=table_schema, properties=table_properties, comment="my comment")

# create table
catalog.create_table(ObjectPath("mydb", "mytable"), catalog_table, False)

# drop table
catalog.drop_table(ObjectPath("mydb", "mytable"), False)

# alter table
catalog.alter_table(ObjectPath("mydb", "mytable"), catalog_table, False)

# rename table
catalog.rename_table(ObjectPath("mydb", "mytable"), "my_new_table")

# get table
catalog.get_table("mytable")

# check if a table exist or not
catalog.table_exists("mytable")

# list tables in a database
catalog.list_tables("mydb")
```

## Useful Flink links
Users can check [Flink Table catalogs docs](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/catalogs.html) to learn more about the general idea of catalog. Users can also find more about Flink SQL Client usage [here](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/sqlClient.html) to interact with catalogs.