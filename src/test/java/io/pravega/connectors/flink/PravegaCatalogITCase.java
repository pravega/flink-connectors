/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pravega.connectors.flink;

import io.pravega.client.stream.EventStreamWriter;
import io.pravega.connectors.flink.table.catalog.pravega.PravegaCatalog;
import io.pravega.connectors.flink.table.catalog.pravega.factories.PravegaCatalogFactoryOptions;
import io.pravega.connectors.flink.utils.SchemaRegistryTestEnvironment;
import io.pravega.connectors.flink.utils.runtime.PravegaRuntime;
import io.pravega.connectors.flink.utils.runtime.SchemaRegistryRuntime;
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
import org.apache.flink.mock.Whitebox;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SuppressWarnings("checkstyle:StaticVariableName")
@Timeout(value = 120)
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
    private static final SchemaRegistryTestEnvironment SCHEMA_REGISTRY =
            new SchemaRegistryTestEnvironment(PravegaRuntime.container(), SchemaRegistryRuntime.container());

    private static PravegaCatalog CATALOG = null;
    private static CatalogTable CATALOG_TABLE = null;

    private final String db1 = "db1";
    private final String t1 = "t1";
    private final String t2 = "t2";
    private final ObjectPath path1 = new ObjectPath(db1, t1);
    private final ObjectPath path2 = new ObjectPath(db1, t2);
    private final CatalogDatabase catalogDb = new CatalogDatabaseImpl(Collections.emptyMap(), null);

    // ------------------------------------------------------------------------

    @BeforeAll
    public static void setupPravega() throws Exception {
        SCHEMA_REGISTRY.startUp();
        init();
        CATALOG.open();
    }

    @AfterAll
    public static void tearDownPravega() throws Exception {
        CATALOG.close();
        SCHEMA_REGISTRY.tearDown();
    }

    @AfterEach
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
        options.put(PravegaCatalogFactoryOptions.DEFAULT_DATABASE.key(), SCHEMA_REGISTRY.operator().getScope());
        options.put(PravegaCatalogFactoryOptions.CONTROLLER_URI.key(), SCHEMA_REGISTRY.operator().getControllerUri().toString());
        options.put(PravegaCatalogFactoryOptions.SCHEMA_REGISTRY_URI.key(), SCHEMA_REGISTRY.schemaRegistryOperator().getSchemaRegistryUri().toString());

        final Catalog actualCatalog = FactoryUtil.createCatalog(TEST_CATALOG_NAME, options, null, Thread.currentThread().getContextClassLoader());

        assertThat(actualCatalog instanceof PravegaCatalog).isTrue();
        assertThat(((PravegaCatalog) actualCatalog).getName()).isEqualTo(CATALOG.getName());
        assertThat(((PravegaCatalog) actualCatalog).getDefaultDatabase()).isEqualTo(CATALOG.getDefaultDatabase());
        assertThat(Whitebox.getInternalState(actualCatalog, "properties"))
                .isEqualTo(Whitebox.getInternalState(CATALOG, "properties"));
    }

    @Test
    public void testCreateDb() throws Exception {
        assertThat(CATALOG.databaseExists(db1)).isFalse();
        CATALOG.createDatabase(db1, catalogDb, false);

        assertThat(CATALOG.databaseExists(db1)).isTrue();
        CatalogTestUtil.checkEquals(catalogDb, CATALOG.getDatabase(db1));
    }

    @Test
    public void testCreateDbAlreadyExist() throws Exception {
        CATALOG.createDatabase(db1, catalogDb, false);
        assertThatThrownBy(() -> CATALOG.createDatabase(db1, catalogDb, false))
                .isInstanceOf(DatabaseAlreadyExistException.class);
    }

    @Test
    public void testCreateDbAlreadyExistIgnore() throws Exception {
        CATALOG.createDatabase(db1, catalogDb, false);
        List<String> dbs = CATALOG.listDatabases();
        assertThat(dbs.size()).isEqualTo(2);
        CATALOG.createDatabase(db1, catalogDb, true);
        dbs = CATALOG.listDatabases();
        assertThat(dbs.size()).isEqualTo(2);
    }

    @Test
    public void testGetDbNotExist() throws Exception {
        assertThatThrownBy(() -> CATALOG.getDatabase("nonexistent"))
                .isInstanceOf(DatabaseNotExistException.class);
    }

    @Test
    public void testDropDb() throws Exception {
        CATALOG.createDatabase(db1, catalogDb, false);
        assertThat(CATALOG.databaseExists(db1)).isTrue();
        CATALOG.dropDatabase(db1, false, true);
        assertThat(CATALOG.databaseExists(db1)).isFalse();
    }

    @Test
    public void testDropDbNotExist() throws Exception {
        assertThatThrownBy(() -> CATALOG.dropDatabase(db1, false, false))
                .isInstanceOf(DatabaseNotExistException.class);
    }

    @Test
    public void testDropDbNotEmpty() throws Exception {
        CATALOG.createDatabase(db1, catalogDb, false);
        CATALOG.createTable(path1, CATALOG_TABLE, false);
        assertThatThrownBy(() -> CATALOG.dropDatabase(db1, true, false))
                .isInstanceOf(DatabaseNotEmptyException.class);
    }

    @Test
    public void testDbExists() throws Exception {
        assertThat(CATALOG.databaseExists("nonexistent")).isFalse();
        CATALOG.createDatabase(db1, catalogDb, false);
        assertThat(CATALOG.databaseExists(db1)).isTrue();
    }

    // ------ tables ------

    @Test
    public void testCreateTable() throws Exception {
        CATALOG.createDatabase(db1, catalogDb, false);
        CATALOG.createTable(path1, CATALOG_TABLE, false);
        registerAvroSchema(db1, t1);
        CatalogTable actual = (CatalogTable) CATALOG.getTable(path1);
        assertThat(actual.getUnresolvedSchema()).isEqualTo(CATALOG_TABLE.getUnresolvedSchema());
        assertThat(actual.getOptions()).isEqualTo(CATALOG_TABLE.getOptions());
    }

    @Test
    public void testCreateTableDbNotExist() throws Exception {
        assertThat(CATALOG.databaseExists(db1)).isFalse();
        assertThatThrownBy(() -> CATALOG.createTable(path1, CATALOG_TABLE, false))
                .isInstanceOf(DatabaseNotExistException.class);
    }

    @Test
    public void testCreateTableAlreadyExist() throws Exception {
        CATALOG.createDatabase(db1, catalogDb, false);
        CATALOG.createTable(path1, CATALOG_TABLE, false);
        registerAvroSchema(db1, t1);
        assertThatThrownBy(() -> CATALOG.createTable(path1, CATALOG_TABLE, false))
                .isInstanceOf(TableAlreadyExistException.class);
    }

    @Test
    public void testCreateTableAlreadyExistIgnore() throws Exception {
        CATALOG.createDatabase(db1, catalogDb, false);
        CATALOG.createTable(path1, CATALOG_TABLE, false);
        registerAvroSchema(db1, t1);
        CATALOG.createTable(path1, CATALOG_TABLE, true);
    }

    @Test
    public void testGetTableNotExist() throws Exception {
        CATALOG.createDatabase(db1, catalogDb, false);
        assertThatThrownBy(() -> CATALOG.getTable(path1))
                .isInstanceOf(TableNotExistException.class);
    }

    @Test
    public void testGetTableDbNotExist() throws Exception {
        assertThatThrownBy(() -> CATALOG.getTable(path1))
                .isInstanceOf(TableNotExistException.class);
    }

    @Test
    public void testDropTable() throws Exception {
        CATALOG.createDatabase(db1, catalogDb, false);
        CATALOG.createTable(path1, CATALOG_TABLE, false);
        registerAvroSchema(db1, t1);
        assertThat(CATALOG.tableExists(path1)).isTrue();

        CATALOG.dropTable(path1, false);

        assertThat(CATALOG.tableExists(path1)).isFalse();
    }

    @Test
    public void testDropTableNotExist() throws Exception {
        assertThatThrownBy(() -> CATALOG.dropTable(path1, false))
                .isInstanceOf(TableNotExistException.class);
    }

    @Test
    public void testDropTableNotExistIgnore() throws Exception {
        CATALOG.createDatabase(db1, catalogDb, false);
        CATALOG.dropTable(path1, true);
    }

    @Test
    public void testListTables() throws Exception {
        CATALOG.createDatabase(db1, catalogDb, false);

        CATALOG.createTable(path1, CATALOG_TABLE, false);
        CATALOG.createTable(path2, CATALOG_TABLE, false);

        assertThat(CATALOG.listTables(db1).size()).isEqualTo(2);
    }

    @Test
    public void testTableExists() throws Exception {
        CATALOG.createDatabase(db1, catalogDb, false);
        assertThat(CATALOG.tableExists(path1)).isFalse();
        CATALOG.createTable(path1, CATALOG_TABLE, false);
        registerAvroSchema(db1, t1);
        assertThat(CATALOG.tableExists(path1)).isTrue();
    }

    // ------ utils ------
    private static void init() throws Exception {
        SCHEMA_REGISTRY.schemaRegistryOperator().registerSchema(TEST_STREAM, AvroSchema.of(TEST_SCHEMA), SerializationFormat.Avro);
        SCHEMA_REGISTRY.operator().createTestStream(TEST_STREAM, 3);
        Map<String, String> properties = new HashMap<>();
        properties.put("connector", "pravega");
        properties.put("controller-uri", SCHEMA_REGISTRY.operator().getControllerUri().toString());
        properties.put("format", "pravega-registry");
        properties.put("pravega-registry.uri",
                SCHEMA_REGISTRY.schemaRegistryOperator().getSchemaRegistryUri().toString());
        properties.put("pravega-registry.format", "Avro");

        CATALOG = new PravegaCatalog(TEST_CATALOG_NAME, SCHEMA_REGISTRY.operator().getScope(), properties,
                SCHEMA_REGISTRY.operator().getPravegaConfig()
                        .withDefaultScope(SCHEMA_REGISTRY.operator().getScope())
                        .withSchemaRegistryURI(SCHEMA_REGISTRY.schemaRegistryOperator().getSchemaRegistryUri()),
                "Avro");
        CATALOG_TABLE = new CatalogTableImpl(TEST_TABLE_SCHEMA, properties, null);
        EventStreamWriter<Object> writer = SCHEMA_REGISTRY.schemaRegistryOperator().getWriter(TEST_STREAM, AvroSchema.of(TEST_SCHEMA), SerializationFormat.Avro);
        writer.writeEvent(EVENT).join();
        writer.close();
    }

    private static void registerAvroSchema(String scope, String stream) throws Exception {
        SchemaRegistryClient client = SchemaRegistryClientFactory.withNamespace(scope,
                SchemaRegistryClientConfig.builder().schemaRegistryUri(SCHEMA_REGISTRY.schemaRegistryOperator().getSchemaRegistryUri()).build());
        SchemaInfo schemaInfo = AvroSchema.of(TEST_SCHEMA).getSchemaInfo();
        client.addSchema(stream, schemaInfo);
        client.close();
    }
}
