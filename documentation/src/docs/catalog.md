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

With the help of schema registry, the Flink connector library for Pravega provides PravegaCatalog by implementing both the the `Catalog` interface and the `CatalogFactory` interface. It also supports serialization/deserialization in schema registry by implementing the `DeserializationFormatFactory` and `SerializationFormatFactory` interface, which enables us to deal with different data formats(Only support Avro format currently).

The catalog factory defines a set of properties for configuring the catalog when the SQL CLI bootstraps. The set of properties will be passed to a discovery service where the service tries to match the properties to a CatalogFactory and initiate a corresponding catalog instance.

## How to use PravegaCatalog

### Create and Register Flink tables to PravegaCatalog

#### Using SQL DDL
Users can use SQL DDL to create tables in catalogs in both Table API and SQL.

##### Java
```java
TableEnvironment tableEnv = ...

// Create a PravegaCatalog 
PravegaCatalog catalog = new PravegaCatalog("pravega", DEFAULT_DATABASE, CONTROLLER_URI, SCHEMAREGISTRY_URI);

// Register the catalog
tableEnv.registerCatalog("pravega", catalog);

// Create a catalog database
tableEnv.executeSql("CREATE DATABASE mydb WITH (...)");

// Create a catalog table
tableEnv.executeSql("CREATE TABLE mytable (name STRING, age INT) WITH (...)");

tableEnv.listTables(); // should return the tables in current catalog and database.
```
##### SQL Client
Start the SQL Client with config yaml
```yaml
execution:
  planner: blink
  type: streaming
  result-mode: table
 
catalogs:
  - name: pravega
    type: pravega
    controller-uri: { CONTROLLER_URI }
    schema-registry-uri: { SCHEMAREGISTRY_URI }
    default-database: { DEFAULT_DATABASE }
```
```shell
./bin/sql-client.sh embedded -e catalog.yaml
```
Inside SQL Client:
```sql
// the catalog should have been registered via yaml file
Flink SQL> CREATE DATABASE mydb WITH (...);

Flink SQL> CREATE TABLE mytable (name STRING, age INT) WITH (...);

// should return the tables in current catalog and database
Flink SQL> SHOW TABLES;
```
#### Using Java
Users can also use Java to create catalog tables programmatically
```java
TableEnvironment tableEnv = ...

// Create a PravegaCatalog
PravegaCatalog catalog = new PravegaCatalog("pravega", DEFAULT_DATABASE, CONTROLLER_URI, SCHEMAREGISTRY_URI);

// Register the catalog
tableEnv.registerCatalog("pravega", catalog);

// Create a catalog database
catalog.createDatabase("mydb", new CatalogDatabaseImpl(...), false);

// Create a catalog table
TableSchema schema = TableSchema.builder()
    .field("name", DataTypes.STRING())
    .field("age", DataTypes.INT())
    .build();

catalog.createTable(
        new ObjectPath("mydb", "mytable"),
        new CatalogTableImpl(
                schema,
                Collections.emptyMap(),
                null),
        false);

List<String> tables = catalog.listTables("mydb"); // tables should contain "mytable"
```

### Example
We will walk through a simple example here to see how Pravega Catalog works by using Flink SQL Client

#### 1. Set up Pravega and Schema Registry
Start your Pravega 0.9 and Schema Registry 0.2.0 with default config.

Inject data into Pravega, we are using Avro format here
```
{
  "type": "record",
  "namespace": "../../avro.generated",
  "name": "User",
  "fields": [
      {"name": "name", "type": "string"},
      {"name": "age", "type": "int"},
      {"name": "info", "type": "string"}
   ]
}
```
Data will be serialized by Schema Registry serializer and then written into Pravega "testScope/testStream"

#### 2. Configure Flink cluster and SQL Client
Download Flink 1.12 cluster https://flink.apache.org/downloads.html

Make sure to add below dependencies to Flink `lib/` directory
- Flink Avro 1.12 https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/connect.html#formats
- Pravega Flink connector 1.12_2.12
- Schema Registry v0.2.0 https://github.com/pravega/schema-registry/releases

Then create your pravega catalog yaml file `catalog.yaml`
```yaml
execution:
  planner: blink
  type: streaming
  result-mode: table
 
catalogs:
  - name: pravega
    type: pravega
    controller-uri: "tcp://localhost:9090"
    schema-registry-uri: "http://localhost:9092"
    default-database: "testScope"
```
Start Flink cluster
```shell
./bin/start-cluster.sh
```

#### 3. Start SQL Client, query the table
```shell
./bin/sql-client.sh embedded -e catalog.yaml
```
You should see catalogs and tables created
```shell
Flink SQL> show catalogs;
default_catalog
pravega

Flink SQL> use catalog pravega;

Flink SQL> show tables;
testStream
```
Run a simple select query
```shell
Flink SQL> select * from testStream;
```
You should see the result produced by Flink in SQL Client now, as:
```sql
 name                       age                      info
   gVh                        28      Jywp2KTiqauzju6LUNfq
   YQz                        84      0CiFOcOrK0fuzVvP5wmy
   OEd                        87      zI80NbPSeK9c1bwpTfck
   JFy                        39      JUZiobRVxgFjSfDfh12V
   KoJ                        24      CxdrYUOV4SAor6gOkdJG
   MbV                        45      I5U5zAfeghv1IQjAbBnM
    .                          .                .
    .                          .                .
    .                          .                .
```

#### 4. Create a table and then query it
Create a new table and verify its schema
```shell
Flink SQL> CREATE TABLE pravega.flinksrtest.testStream2 (
>     name STRING,
>     age INT,
>     info STRING
> );
[INFO] Table has been created.

Flink SQL> desc testStream2;
+------+--------+------+-----+--------+-----------+
| name |   type | null | key | extras | watermark |
+------+--------+------+-----+--------+-----------+
| name | STRING | true |     |        |           |
|  age |    INT | true |     |        |           |
| info | STRING | true |     |        |           |
+------+--------+------+-----+--------+-----------+
3 rows in set

```

Then we insert data from testStream to testStream2 and then run a simple select query
```shell
Flink SQL> INSERT INTO testStream2
> SELECT * FROM testStream;
[INFO] Submitting SQL update statement to the cluster...
[INFO] Table update statement has been successfully submitted to the cluster:
Job ID: beb2ead7a52f98db1c4d3ad2c7553e1b
```
```shell
Flink SQL> select * from testStream2;
```
Query result:
```sql
 name                       age                      info
   UnC                        27      SrXBynLOBX1B2sPfZEgX
   dcX                        32      zLyAxW40E3ZpSRyZJdq2
   Cfh                        46      70ar7NxJxi5mw1OyWMQV
   EwB                        29      yMg3bfnsx6AKIqJ0DCG5
   hui                        97      5meAuFirQDXwQ3VZfMGf
   dDd                        41      khJAEPo4khL2n2zFkpjn
    .                          .                .
    .                          .                .
    .                          .                .
```


## Catalog API
Only catalog program APIs are listed here. Users can achieve many of the same funtionalities with SQL DDL. For detailed DDL information, please refer to [SQL CREATE DDL](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/sql/create.html).

Only database and table operations are supported now, views/partitions/functions/statistics operations are NOT supported in PravegaCatalog

### Database operations
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
### Table operations
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

## Useful Flink links
Users can check [Flink Table catalogs docs](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/catalogs.html) to learn more about the general idea of catalog. Users can also find more about Flink SQL Client usage [here](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/sqlClient.html) to interact with catalogs.