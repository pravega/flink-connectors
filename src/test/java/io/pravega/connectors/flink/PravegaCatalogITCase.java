/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink;

import io.pravega.client.stream.EventStreamWriter;
import io.pravega.connectors.flink.table.catalog.pravega.PravegaCatalog;
import io.pravega.connectors.flink.table.catalog.pravega.factories.PravegaCatalogFactoryOptions;
import io.pravega.connectors.flink.utils.SchemaRegistryUtils;
import io.pravega.connectors.flink.utils.SetupUtils;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.CatalogTestUtil;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("checkstyle:StaticVariableName")
public class PravegaCatalogITCase {
    private static final Schema TEST_SCHEMA = SchemaBuilder
            .record("MyTest")
            .fields()
            .name("a")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .endRecord();
    private static final TableSchema TEST_TABLE_SCHEMA = TableSchema.builder()
            .field("a", DataTypes.STRING())
            .build();
    private static final String TEST_CATALOG_NAME = "mycatalog";
    private static final String TEST_STREAM = "stream";
    private static final GenericRecord EVENT = new GenericRecordBuilder(TEST_SCHEMA).set("a", "test").build();

    /** Setup utility */
    private static final SetupUtils SETUP_UTILS = new SetupUtils();
    private static final SchemaRegistryUtils SCHEMA_REGISTRY_UTILS =
            new SchemaRegistryUtils(SETUP_UTILS, SchemaRegistryUtils.DEFAULT_PORT);

    private static PravegaCatalog CATALOG = null;

    @Rule
    public final Timeout globalTimeout = new Timeout(120, TimeUnit.SECONDS);

    private final String db1 = "db1";
    private final String t1 = "t1";
    private final String t2 = "t2";
    private final ObjectPath path1 = new ObjectPath(db1, t1);
    private final ObjectPath path2 = new ObjectPath(db1, t2);
    private final CatalogDatabase catalogDb = new CatalogDatabaseImpl(Collections.emptyMap(), null);
    private final CatalogTable catalogTable = new CatalogTableImpl(TEST_TABLE_SCHEMA, Collections.emptyMap(), null);
    // ------------------------------------------------------------------------

    @BeforeClass
    public static void setupPravega() throws Exception {
        SETUP_UTILS.startAllServices();
        SCHEMA_REGISTRY_UTILS.setupServices();
        init();
        CATALOG.open();
    }

    @AfterClass
    public static void tearDownPravega() throws Exception {
        CATALOG.close();
        SETUP_UTILS.stopAllServices();
        SCHEMA_REGISTRY_UTILS.tearDownServices();
    }

    @After
    public void cleanup() throws Exception {
        if (CATALOG.tableExists(path1)) {
            CATALOG.dropTable(path1, true);
        }
        if (CATALOG.tableExists(path2)) {
            CATALOG.dropTable(path2, true);
        }
        if (CATALOG.databaseExists(db1)) {
            CATALOG.dropDatabase(db1, true, false);
        }
    }

    @Test
    public void testCreateCatalogFromFactory() {
        final Map<String, String> options = new HashMap<>();
        options.put(CommonCatalogOptions.CATALOG_TYPE.key(), PravegaCatalogFactoryOptions.IDENTIFIER);
        options.put(PravegaCatalogFactoryOptions.DEFAULT_DATABASE.key(), SETUP_UTILS.getScope());
        options.put(PravegaCatalogFactoryOptions.CONTROLLER_URI.key(), SETUP_UTILS.getControllerUri().toString());
        options.put(PravegaCatalogFactoryOptions.SCHEMA_REGISTRY_URI.key(), SCHEMA_REGISTRY_UTILS.getSchemaRegistryUri().toString());

        final Catalog actualCatalog = FactoryUtil.createCatalog(TEST_CATALOG_NAME, options, null, Thread.currentThread().getContextClassLoader());

        assertTrue(actualCatalog instanceof PravegaCatalog);
        assertEquals(((PravegaCatalog) actualCatalog).getName(), CATALOG.getName());
        assertEquals(((PravegaCatalog) actualCatalog).getDefaultDatabase(), CATALOG.getDefaultDatabase());
    }

    @Test
    public void testCreateDb() throws Exception {
        assertFalse(CATALOG.databaseExists(db1));
        CATALOG.createDatabase(db1, catalogDb, false);

        assertTrue(CATALOG.databaseExists(db1));
        CatalogTestUtil.checkEquals(catalogDb, CATALOG.getDatabase(db1));
    }

    @Test(expected = DatabaseAlreadyExistException.class)
    public void testCreateDbAlreadyExist() throws Exception {
        CATALOG.createDatabase(db1, catalogDb, false);
        CATALOG.createDatabase(db1, catalogDb, false);
    }

    @Test
    public void testCreateDbAlreadyExistIgnore() throws Exception {
        CATALOG.createDatabase(db1, catalogDb, false);
        List<String> dbs = CATALOG.listDatabases();
        assertEquals(2, dbs.size());
        CATALOG.createDatabase(db1, catalogDb, true);
        dbs = CATALOG.listDatabases();
        assertEquals(2, dbs.size());
    }

    @Test(expected = DatabaseNotExistException.class)
    public void testGetDbNotExist() throws Exception {
        CATALOG.getDatabase("nonexistent");
    }

    @Test
    public void testDropDb() throws Exception {
        CATALOG.createDatabase(db1, catalogDb, false);
        assertTrue(CATALOG.databaseExists(db1));
        CATALOG.dropDatabase(db1, false, true);
        assertFalse(CATALOG.databaseExists(db1));
    }

    @Test(expected = DatabaseNotExistException.class)
    public void testDropDbNotExist() throws Exception {
        CATALOG.dropDatabase(db1, false, false);
    }

    @Test(expected = DatabaseNotEmptyException.class)
    public void testDropDbNotEmpty() throws Exception {
        CATALOG.createDatabase(db1, catalogDb, false);
        CATALOG.createTable(path1, catalogTable, false);
        CATALOG.dropDatabase(db1, true, false);
    }

    @Test
    public void testDbExists() throws Exception {
        assertFalse(CATALOG.databaseExists("nonexistent"));
        CATALOG.createDatabase(db1, catalogDb, false);
        assertTrue(CATALOG.databaseExists(db1));
    }

    // ------ tables ------

    @Test
    public void testCreateTable() throws Exception {
        CATALOG.createDatabase(db1, catalogDb, false);
        CATALOG.createTable(path1, catalogTable, false);
        registerAvroSchema(db1, t1);
        CatalogTable actual = (CatalogTable) CATALOG.getTable(path1);
        Assert.assertEquals(catalogTable.getClass(), actual.getClass());
        Assert.assertEquals(catalogTable.getSchema(), actual.getSchema());
    }

    @Test(expected = DatabaseNotExistException.class)
    public void testCreateTableDbNotExist() throws Exception {
        assertFalse(CATALOG.databaseExists(db1));
        CATALOG.createTable(path1, catalogTable, false);
    }

    @Test(expected = TableAlreadyExistException.class)
    public void testCreateTableAlreadyExist() throws Exception {
        CATALOG.createDatabase(db1, catalogDb, false);
        CATALOG.createTable(path1, catalogTable, false);
        registerAvroSchema(db1, t1);
        CATALOG.createTable(path1, catalogTable, false);
    }

    @Test
    public void testCreateTableAlreadyExistIgnore() throws Exception {
        CATALOG.createDatabase(db1, catalogDb, false);
        CATALOG.createTable(path1, catalogTable, false);
        registerAvroSchema(db1, t1);
        CATALOG.createTable(path1, catalogTable, true);
    }

    @Test(expected = TableNotExistException.class)
    public void testGetTableNotExist() throws Exception {
        CATALOG.createDatabase(db1, catalogDb, false);
        CATALOG.getTable(path1);
    }

    @Test(expected = TableNotExistException.class)
    public void testGetTableDbNotExist() throws Exception {
        CATALOG.getTable(path1);
    }

    @Test
    public void testDropTable() throws Exception {
        CATALOG.createDatabase(db1, catalogDb, false);
        CATALOG.createTable(path1, catalogTable, false);
        registerAvroSchema(db1, t1);
        assertTrue(CATALOG.tableExists(path1));

        CATALOG.dropTable(path1, false);

        assertFalse(CATALOG.tableExists(path1));
    }

    @Test(expected = TableNotExistException.class)
    public void testDropTableNotExist() throws Exception {
        CATALOG.dropTable(path1, false);
    }

    @Test
    public void testDropTableNotExistIgnore() throws Exception {
        CATALOG.createDatabase(db1, catalogDb, false);
        CATALOG.dropTable(path1, true);
    }

    @Test
    public void testListTables() throws Exception {
        CATALOG.createDatabase(db1, catalogDb, false);

        CATALOG.createTable(path1, catalogTable, false);
        CATALOG.createTable(path2, catalogTable, false);

        assertEquals(2, CATALOG.listTables(db1).size());
    }

    @Test
    public void testTableExists() throws Exception {
        CATALOG.createDatabase(db1, catalogDb, false);
        assertFalse(CATALOG.tableExists(path1));
        CATALOG.createTable(path1, catalogTable, false);
        registerAvroSchema(db1, t1);
        assertTrue(CATALOG.tableExists(path1));
    }

    // ------ utils ------
    private static void init() throws Exception {
        SCHEMA_REGISTRY_UTILS.registerSchema(TEST_STREAM, AvroSchema.of(TEST_SCHEMA), SerializationFormat.Avro);
        SETUP_UTILS.createTestStream(TEST_STREAM, 3);
        Map<String, String> properties = new HashMap<>();
        properties.put("connector", "pravega");
        properties.put("controller-uri", SETUP_UTILS.getControllerUri().toString());
        properties.put("format", "pravega-registry");
        properties.put("pravega-registry.uri",
                SCHEMA_REGISTRY_UTILS.getSchemaRegistryUri().toString());
        properties.put("pravega-registry.format", "Avro");
        CATALOG = new PravegaCatalog(TEST_CATALOG_NAME, SETUP_UTILS.getScope(), properties,
                SETUP_UTILS.getPravegaConfig()
                        .withDefaultScope(SETUP_UTILS.getScope())
                        .withSchemaRegistryURI(SCHEMA_REGISTRY_UTILS.getSchemaRegistryUri()),
                "Avro");
        EventStreamWriter<Object> writer = SCHEMA_REGISTRY_UTILS.getWriter(TEST_STREAM, AvroSchema.of(TEST_SCHEMA), SerializationFormat.Avro);
        writer.writeEvent(EVENT).join();
        writer.close();
    }

    private static void registerAvroSchema(String scope, String stream) throws Exception {
        SchemaRegistryClient client = SchemaRegistryClientFactory.withNamespace(scope,
                SchemaRegistryClientConfig.builder().schemaRegistryUri(SCHEMA_REGISTRY_UTILS.getSchemaRegistryUri()).build());
        SchemaInfo schemaInfo = AvroSchema.of(TEST_SCHEMA).getSchemaInfo();
        client.addSchema(stream, schemaInfo);
        client.close();
    }
}
