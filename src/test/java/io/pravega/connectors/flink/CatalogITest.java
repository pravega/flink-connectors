package io.pravega.connectors.flink;

import io.pravega.client.stream.EventStreamWriter;
import io.pravega.connectors.flink.table.catalog.pravega.PravegaCatalog;
import io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogDescriptor;
import io.pravega.connectors.flink.utils.SchemaRegistryUtils;
import io.pravega.connectors.flink.utils.SetupUtils;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.cli.Options;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.CatalogTestUtil;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.local.ExecutionContext;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.descriptors.CatalogDescriptor;
import org.apache.flink.table.descriptors.GenericInMemoryCatalogDescriptor;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@Slf4j
public class CatalogITest {
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

    private final String db1 = "db1";
    protected final String t1 = "t1";
    protected final ObjectPath path1 = new ObjectPath(db1, t1);

    private final CatalogDatabase catalogDb = new CatalogDatabaseImpl(Collections.emptyMap(), null);
    private final CatalogTable catalogTable = new CatalogTableImpl(TEST_TABLE_SCHEMA, Collections.emptyMap(), null);
    private static PravegaCatalog catalog;

    /** Setup utility */
    private static final SetupUtils SETUP_UTILS = new SetupUtils();
    private static final SchemaRegistryUtils SCHEMA_REGISTRY_UTILS =
            new SchemaRegistryUtils(SETUP_UTILS, SchemaRegistryUtils.DEFAULT_PORT);

    @Rule
    public final Timeout globalTimeout = new Timeout(120, TimeUnit.SECONDS);

    // ------------------------------------------------------------------------

    @BeforeClass
    public static void setupPravega() throws Exception {
        SETUP_UTILS.startAllServices();
        SCHEMA_REGISTRY_UTILS.setupServices();
        catalog = new PravegaCatalog(TEST_CATALOG_NAME, SETUP_UTILS.getScope(),
                SETUP_UTILS.getControllerUri().toString(), SCHEMA_REGISTRY_UTILS.getSchemaRegistryUri().toString());
        init();
        catalog.open();
    }

    @AfterClass
    public static void tearDownPravega() throws Exception {
        catalog.close();
        SETUP_UTILS.stopAllServices();
        SCHEMA_REGISTRY_UTILS.tearDownServices();
    }

    @After
    public void cleanup() throws Exception {
        if (catalog.tableExists(path1)) {
            catalog.dropTable(path1, true);
        }
//        if (catalog.tableExists(path2)) {
//            catalog.dropTable(path2, true);
//        }
//        if (catalog.tableExists(path3)) {
//            catalog.dropTable(path3, true);
//        }
//        if (catalog.tableExists(path4)) {
//            catalog.dropTable(path4, true);
//        }
        if (catalog.databaseExists(db1)) {
            catalog.dropDatabase(db1, true, false);
        }
//        if (catalog.databaseExists(db2)) {
//            catalog.dropDatabase(db2, true, false);
//        }
    }

    @Test
    public void testCreateCatalogFromFactory() {

        final CatalogDescriptor catalogDescriptor = new PravegaCatalogDescriptor(
                SETUP_UTILS.getControllerUri().toString(),
                SCHEMA_REGISTRY_UTILS.getSchemaRegistryUri().toString(),
                SETUP_UTILS.getScope());
        final Map<String, String> properties = catalogDescriptor.toProperties();

        final Catalog actualCatalog = TableFactoryService.find(CatalogFactory.class, properties)
                .createCatalog(TEST_CATALOG_NAME, properties);

        assertTrue(actualCatalog instanceof PravegaCatalog);
        assertEquals(((PravegaCatalog) actualCatalog).getName(), catalog.getName());
        assertEquals(((PravegaCatalog) actualCatalog).getDefaultDatabase(), catalog.getDefaultDatabase());
    }

    @Test
    public void testCreateDb() throws Exception {
        assertFalse(catalog.databaseExists(db1));
        catalog.createDatabase(db1, catalogDb, false);

        assertTrue(catalog.databaseExists(db1));
        CatalogTestUtil.checkEquals(catalogDb, catalog.getDatabase(db1));
    }

    @Test(expected = DatabaseAlreadyExistException.class)
    public void testCreateDbAlreadyExist() throws Exception {
        catalog.createDatabase(db1, catalogDb, false);
        catalog.createDatabase(db1, catalogDb, false);
    }

    @Test
    public void testCreateDbAlreadyExistIgnore() throws Exception {
        catalog.createDatabase(db1, catalogDb, false);
        List<String> dbs = catalog.listDatabases();
        assertEquals(2, dbs.size());
        catalog.createDatabase(db1, catalogDb, true);
        dbs = catalog.listDatabases();
        assertEquals(2, dbs.size());
    }

    @Test(expected = DatabaseNotExistException.class)
    public void testGetDbNotExist() throws Exception {
        catalog.getDatabase("nonexistent");
    }

    @Test
    public void testDropDb() throws Exception {
        catalog.createDatabase(db1, catalogDb, false);
        assertTrue(catalog.databaseExists(db1));
        catalog.dropDatabase(db1, false, true);
        assertFalse(catalog.databaseExists(db1));
    }

    @Test(expected = DatabaseNotExistException.class)
    public void testDropDbNotExist() throws Exception {
        catalog.dropDatabase(db1, false, false);
    }

    @Test(expected = DatabaseNotEmptyException.class)
    public void testDropDbNotEmpty() throws Exception {
        catalog.createDatabase(db1, catalogDb, false);
        catalog.createTable(path1, catalogTable, false);
        catalog.dropDatabase(db1, true, false);
    }

    @Test
    public void testDbExists() throws Exception {
        assertFalse(catalog.databaseExists("nonexistent"));
        catalog.createDatabase(db1, catalogDb, false);
        assertTrue(catalog.databaseExists(db1));
    }

    // ------ tables ------

    @Test
    public void testCreateTable() throws Exception {
        catalog.createDatabase(db1, catalogDb, false);
        catalog.createTable(path1, catalogTable, false);
        registerAvroSchema(db1, t1);
        CatalogTable actual = (CatalogTable) catalog.getTable(path1);
        Assert.assertEquals(catalogTable.getClass(), actual.getClass());
        Assert.assertEquals(catalogTable.getSchema(), actual.getSchema());
    }


    // ------ utils ------
    private static void init() throws Exception {
        SCHEMA_REGISTRY_UTILS.registerAvroSchema(TEST_STREAM, TEST_SCHEMA);
        SETUP_UTILS.createTestStream(TEST_STREAM, 3);
        EventStreamWriter<Object> writer = SCHEMA_REGISTRY_UTILS.getWriter(TEST_STREAM, TEST_SCHEMA);
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
