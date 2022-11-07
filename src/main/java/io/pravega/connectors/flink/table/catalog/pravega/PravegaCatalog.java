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

package io.pravega.connectors.flink.table.catalog.pravega;

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.DeleteScopeFailedException;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.dynamic.table.FlinkPravegaDynamicTableFactory;
import io.pravega.connectors.flink.dynamic.table.PravegaOptions;
import io.pravega.connectors.flink.formats.registry.PravegaRegistryFormatFactory;
import io.pravega.connectors.flink.formats.registry.PravegaRegistryOptions;
import io.pravega.connectors.flink.table.catalog.pravega.util.PravegaSchemaUtils;
import io.pravega.connectors.flink.util.SchemaRegistryUtils;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class PravegaCatalog extends AbstractCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(PravegaCatalog.class);

    // the Pravega stream manager to manage streams
    private StreamManager streamManager;

    // the schema-registry client that communicates with schema-registry service
    private SchemaRegistryClient schemaRegistryClient;

    // the Pravega client config
    private ClientConfig clientConfig;

    // the schema-registry client config
    private SchemaRegistryClientConfig config;

    // the table options that should propagate from the catalog options.
    private Map<String, String> properties;

    // the serialization format for Pravega catalog
    private SerializationFormat serializationFormat;

    /**
     * Creates a new Pravega Catalog instance.
     *
     * @param catalogName             The Pravega catalog name.
     * @param defaultDatabase         The default database for Pravega catalog, which is mapped to Pravega scope here.
     * @param properties              The table options that should propagate from the catalog options.
     * @param pravegaConfig           The Pravega configuration.
     * @param serializationFormat     The serialization format used for serialization.
     */
    public PravegaCatalog(String catalogName, String defaultDatabase, Map<String, String> properties,
                          PravegaConfig pravegaConfig, String serializationFormat) {

        super(catalogName, defaultDatabase);
        this.clientConfig = pravegaConfig.getClientConfig();
        this.config = SchemaRegistryUtils.getSchemaRegistryClientConfig(pravegaConfig);
        this.serializationFormat = SerializationFormat.valueOf(serializationFormat);
        this.properties = properties;

        LOG.info("Created Pravega Catalog {}", catalogName);
    }

    @Override
    public void open() throws CatalogException {
        if (streamManager == null) {
            try {
                streamManager = StreamManager.create(clientConfig);
            } catch (Exception e) {
                throw new CatalogException("Failed to connect Pravega controller");
            }
        }

        LOG.info("Connected to Pravega controller");

        if (!databaseExists(getDefaultDatabase())) {
            throw new CatalogException(String.format("Configured default database %s doesn't exist in catalog %s.",
                    getDefaultDatabase(), getName()));
        }

        if (schemaRegistryClient == null) {
            try {
                schemaRegistryClient = SchemaRegistryClientFactory.withNamespace(getDefaultDatabase(), config);
            } catch (Exception e) {
                throw new CatalogException("Failed to connect Pravega Schema Registry");
            }
        }

        LOG.info("Connected to Pravega Schema Registry");
    }

    @Override
    public void close() throws CatalogException {
        if (streamManager != null) {
            streamManager.close();
            streamManager = null;
            LOG.info("Close connection to Pravega");
        }

        if (schemaRegistryClient != null) {
            try {
                schemaRegistryClient.close();
                LOG.info("Close connection to Pravega Schema registry");
            } catch (Exception e) {
                throw new CatalogException("Fail to close connection to Pravega Schema registry", e);
            } finally {
                schemaRegistryClient = null;
            }
        }
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new FlinkPravegaDynamicTableFactory());
    }

    // ------ databases ------

    @Override
    public List<String> listDatabases() throws CatalogException {

        Iterable<String> iterable = () -> streamManager.listScopes();

        List<String> databaseList = StreamSupport
                .stream(iterable.spliterator(), false)
                .filter(s -> !s.startsWith("_"))
                .collect(Collectors.toList());

        return databaseList;
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        return new CatalogDatabaseImpl(Collections.emptyMap(), null);
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return streamManager.checkScopeExists(databaseName);
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists) throws DatabaseAlreadyExistException, CatalogException {
        if (databaseExists(name)) {
            if (!ignoreIfExists) {
                throw new DatabaseAlreadyExistException(getName(), name);
            }
            return;
        }
        streamManager.createScope(name);
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade) throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        if (!databaseExists(name)) {
            if (!ignoreIfNotExists) {
                throw new DatabaseNotExistException(getName(), name);
            }
            return;
        }

        changeRegistryNamespace(name);
        if (listTables(name).size() != 0) {
            if (cascade) {
                listTables(name).forEach(schemaRegistryClient::removeGroup);
            } else {
                throw new DatabaseNotEmptyException(getName(), name);
            }
        }

        try {
            streamManager.deleteScopeRecursive(name);
        } catch (DeleteScopeFailedException e) {
            throw new CatalogException(String.format("Failed to drop database %s", name));
        }
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists) throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    // ------ tables ------

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        Iterable<Stream> iterable = () -> streamManager.listStreams(databaseName);

        Set<String> groupSet = new HashSet<>();
        changeRegistryNamespace(databaseName);
        schemaRegistryClient.listGroups().forEachRemaining( kv -> {
            groupSet.add(kv.getKey());
        });

        return StreamSupport
                .stream(iterable.spliterator(), false)
                .map(Stream::getStreamName)
                .filter(s -> !s.startsWith("_"))
                .filter(groupSet::contains)
                .collect(Collectors.toList());
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }

        String scope = tablePath.getDatabaseName();
        String stream = tablePath.getObjectName();
        changeRegistryNamespace(scope);
        SchemaInfo schemaInfo = schemaRegistryClient.getLatestSchemaVersion(stream, null).getSchemaInfo();
        ResolvedSchema resolvedSchema = PravegaSchemaUtils.schemaInfoToResolvedSchema(schemaInfo);

        Map<String, String> properties = this.properties;
        properties.put(PravegaOptions.SCOPE.key(), scope);
        properties.put(PravegaOptions.SCAN_STREAMS.key(), stream);
        properties.put(PravegaOptions.SINK_STREAM.key(), stream);

        // schema registry treats Pravega scope as namespace and Pravega stream as group-id
        properties.put(
                String.format(
                        "%s.%s",
                        PravegaRegistryFormatFactory.IDENTIFIER, PravegaRegistryOptions.NAMESPACE.key()),
                scope);
        properties.put(
                String.format(
                        "%s.%s",
                        PravegaRegistryFormatFactory.IDENTIFIER, PravegaRegistryOptions.GROUP_ID.key()),
                stream);

        return CatalogTable.of(Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                "", Collections.emptyList(), properties);
//        return new CatalogTableImpl(TableSchema.fromResolvedSchema(resolvedSchema), properties, "");
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        String scope = tablePath.getDatabaseName();
        String stream = tablePath.getObjectName();
        return streamManager.checkStreamExists(scope, stream);
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(getName(), tablePath);
            }
            return;
        }

        String scope = tablePath.getDatabaseName();
        String stream = tablePath.getObjectName();
        streamManager.sealStream(scope, stream);
        streamManager.deleteStream(scope, stream);
        changeRegistryNamespace(scope);
        try {
            schemaRegistryClient.removeGroup(stream);
        } catch (Exception e) {
            throw new CatalogException(String.format("Fail to drop table %s/%s", scope, stream), e);
        }
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
            if (!ignoreIfExists) {
                throw new TableAlreadyExistException(getName(), tablePath);
            }
            return;
        }

        streamManager.createStream(scope, stream, StreamConfiguration.builder().build());
        changeRegistryNamespace(scope);
        schemaRegistryClient.addGroup(stream, new GroupProperties(
                serializationFormat,
                Compatibility.allowAny(),
                true));

        SchemaInfo schemaInfo = PravegaSchemaUtils.tableSchemaToSchemaInfo(
                table.getUnresolvedSchema(), serializationFormat);
        schemaRegistryClient.addSchema(stream, schemaInfo);
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    // ------ partitions/functions/statistics NOT supported ------

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> filters) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return false;
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
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
        throw new FunctionNotExistException(getName(), functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
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

    private void changeRegistryNamespace(String namespace) {
        // TODO: If the client keeps the same namespace, it is redundant.
        try {
            schemaRegistryClient.close();
        } catch (Exception e) {
            throw new CatalogException("Fail to close connection to Pravega Schema registry", e);
        }
        schemaRegistryClient = SchemaRegistryClientFactory.withNamespace(namespace, config);
    }
}
