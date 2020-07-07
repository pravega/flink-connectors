package io.pravega.connectors.flink.table.catalog.pravega;

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.connectors.flink.table.catalog.pravega.util.PravegaSchemaUtils;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.FunctionDefinitionFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.util.StringUtils;

import java.net.URI;
import java.util.*;

import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.CATALOG_CONTROLLER_URI;
import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.CATALOG_SCHEMA_REGISTRY_URI;
import static io.pravega.connectors.flink.table.descriptors.Pravega.*;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_TYPE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_APPEND;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

@Slf4j
public class PravegaCatalog extends AbstractCatalog {
    private StreamManager streamManager;
    private SchemaRegistryClient schemaRegistryClient;
    private final URI controllerUri;
    private final URI schemaRegistryUri;
    private Map<String, String> properties;

    public PravegaCatalog(String catalogName, String defaultDatabase, String controllerUri, String schemaRegistryUri) {

        super(catalogName, defaultDatabase);

        this.controllerUri = URI.create(controllerUri);
        this.schemaRegistryUri = URI.create(schemaRegistryUri);

        this.properties = new HashMap<>();
        properties.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_PRAVEGA);
        properties.put(CONNECTOR_VERSION, String.valueOf(CONNECTOR_VERSION_VALUE));
        properties.put(CONNECTOR_CONNECTION_CONFIG_CONTROLLER_URI, controllerUri);
        properties.put(CONNECTOR_CONNECTION_CONFIG_DEFAULT_SCOPE, defaultDatabase);

        log.info("Created Pravega Catalog {}", catalogName);
    }

    @Override
    public void open() throws CatalogException {
        if (streamManager == null) {
            try {
                streamManager = StreamManager.create(controllerUri);
            } catch (Exception e) {
                throw new CatalogException("Failed to connect Pravega controller");
            }
        }

        log.info("Connected to Pravega controller");

        if (!databaseExists(getDefaultDatabase())) {
            throw new CatalogException(String.format("Configured default database %s doesn't exist in catalog %s.",
                    getDefaultDatabase(), getName()));
        }

        if (schemaRegistryClient == null) {
            try {
                SchemaRegistryClientConfig config = SchemaRegistryClientConfig.builder()
                        .schemaRegistryUri(schemaRegistryUri).namespace(getDefaultDatabase()).build();
                schemaRegistryClient = SchemaRegistryClientFactory.createRegistryClient(config);
            } catch (Exception e) {
                throw new CatalogException("Failed to connect Pravega Schema Registry");
            }
        }

        log.info("Connected to Pravega Schema Registry");
    }

    @Override
    public void close() throws CatalogException {
        if (streamManager != null) {
            streamManager.close();
            streamManager = null;
            log.info("Close connection to Pravega");
        }

        // TODO: schema registry client close
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        // TODO: listDatabases
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<TableFactory> getTableFactory() {
        // TODO: now only streaming read
        TableFactory tableFactory = TableFactoryService.find(StreamTableSourceFactory.class, properties);
        return Optional.of(tableFactory);
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        // Switch the schema registry client to the new namespace
        // schemaRegistryClient.close();
        SchemaRegistryClientConfig config = SchemaRegistryClientConfig.builder()
                .schemaRegistryUri(schemaRegistryUri).namespace(databaseName).build();
        schemaRegistryClient = SchemaRegistryClientFactory.createRegistryClient(config);

        return new CatalogDatabaseImpl(Collections.emptyMap(), null);
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        // TODO: databaseExists
        return true;
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists) throws DatabaseAlreadyExistException, CatalogException {
        boolean isExists;
        try {
            isExists = streamManager.createScope(name);
        } catch (Exception e) {
            throw new CatalogException(String.format("Failed to create database %s", name), e);
        }
        if (isExists && !ignoreIfExists) {
            throw new DatabaseAlreadyExistException(getName(), name);
        }
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade) throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        if (!databaseExists(name)) {
            if (ignoreIfNotExists) {
                return;
            } else {
                throw new DatabaseNotExistException(getName(), name);
            }
        }

        if (!isDatabaseEmpty(name)) {
            if (cascade) {
                Iterator<Stream> iterator = streamManager.listStreams(name);
                iterator.forEachRemaining( stream -> {
                    streamManager.deleteStream(name, stream.getStreamName());
                });
            } else {
                throw new DatabaseNotEmptyException(getName(), name);
            }
        }

        if (!streamManager.deleteScope(name)) {
            throw new CatalogException(String.format("Failed to create database %s", name));
        }
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists) throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        List<String> result = new ArrayList<>();

        Iterator<Stream> iterator = streamManager.listStreams(databaseName);
        Set<String> groupSet = new HashSet<>();
        schemaRegistryClient.listGroups().forEachRemaining( kv -> {
            groupSet.add(kv.getKey());
        });
        iterator.forEachRemaining( stream -> {
            String table = stream.getStreamName();
            if (groupSet.contains(table)) {
                result.add(table);
            }
        } );

        return result;
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }

        String stream = tablePath.getObjectName();
        SchemaInfo schemaInfo = schemaRegistryClient.getLatestSchemaVersion(stream, null).getSchemaInfo();
        TableSchema tableSchema = PravegaSchemaUtils.schemaInfoToTableSchema(schemaInfo);

        Map<String, String> properties = this.properties;
        properties.put(CONNECTOR_READER_STREAM_INFO + ".0." + CONNECTOR_READER_STREAM_INFO_STREAM, stream);
        properties.put(FORMAT_TYPE, schemaInfo.getSerializationFormat().name().toLowerCase());
        properties.put(UPDATE_MODE, UPDATE_MODE_VALUE_APPEND);

        return new CatalogTableImpl(tableSchema, properties, "Pravega Catalog Table");
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        String scope = tablePath.getDatabaseName();
        String stream = tablePath.getObjectName();

        // TODO: table exists
        return true;
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (ignoreIfNotExists) {
                return;
            } else {
                throw new TableNotExistException(getName(), tablePath);
            }
        }

        String scope = tablePath.getDatabaseName();
        String stream = tablePath.getObjectName();
        streamManager.deleteStream(scope, stream);
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) throws TableNotExistException, TableAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        String scope = tablePath.getDatabaseName();
        String stream = tablePath.getObjectName();
        if (!databaseExists(scope)) {
            throw new DatabaseNotExistException(getName(), scope);
        }

        if (tableExists(tablePath)) {
            if (ignoreIfExists) {
                return;
            } else {
                throw new TableAlreadyExistException(getName(), tablePath);
            }
        }

        streamManager.createStream(scope, stream, StreamConfiguration.builder().build());

        // TODO: register the schema
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> filters) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return null;
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition, boolean ignoreIfExists) throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listFunctions(String dbName) throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists) throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException, TablePartitionedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    // Helper methods
    private boolean isDatabaseEmpty(String databaseName) throws CatalogException, DatabaseNotExistException {
        return listTables(databaseName).size() == 0;
    }
}
