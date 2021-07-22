/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink.table.catalog.pravega;

import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.DeleteScopeFailedException;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.connectors.flink.dynamic.table.FlinkPravegaDynamicTableFactory;
import io.pravega.connectors.flink.dynamic.table.PravegaOptions;
import io.pravega.connectors.flink.formats.registry.PravegaRegistryFormatFactory;
import io.pravega.connectors.flink.formats.registry.PravegaRegistryOptions;
import io.pravega.connectors.flink.table.catalog.pravega.util.PravegaSchemaUtils;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
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
import org.apache.flink.table.factories.FactoryUtil;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Slf4j
public class PravegaCatalog extends AbstractCatalog {
    private StreamManager streamManager;
    private SchemaRegistryClient schemaRegistryClient;
    private SchemaRegistryClientConfig config;
    private final URI controllerUri;
    private final URI schemaRegistryUri;
    private Map<String, String> properties;
    private SerializationFormat serializationFormat;

    public PravegaCatalog(String catalogName, String defaultDatabase, String controllerUri, String schemaRegistryUri,
                          String serializationFormat, String failOnMissingField, String ignoreParseErrors,
                          String timestampFormat, String mapNullKeyMode, String mapNullKeyLiteral) {

        super(catalogName, defaultDatabase);

        this.controllerUri = URI.create(controllerUri);
        this.schemaRegistryUri = URI.create(schemaRegistryUri);
        this.config = SchemaRegistryClientConfig.builder()
                .schemaRegistryUri(this.schemaRegistryUri).build();
        this.serializationFormat = serializationFormat == null ?
                SerializationFormat.Avro : SerializationFormat.valueOf(serializationFormat);

        this.properties = new HashMap<>();
        properties.put(FactoryUtil.CONNECTOR.key(), FlinkPravegaDynamicTableFactory.IDENTIFIER);
        properties.put(PravegaOptions.CONTROLLER_URI.key(), controllerUri);
        properties.put(FactoryUtil.FORMAT.key(), PravegaRegistryFormatFactory.IDENTIFIER);
        properties.put(
                String.format(
                        "%s.%s",
                        PravegaRegistryFormatFactory.IDENTIFIER, PravegaRegistryOptions.URI.key()),
        schemaRegistryUri);

        properties.put(String.format("%s.%s",
                PravegaRegistryFormatFactory.IDENTIFIER, PravegaRegistryOptions.FORMAT.key()),
                this.serializationFormat.name());

        propagateJsonOptions(failOnMissingField, ignoreParseErrors, timestampFormat, mapNullKeyMode, mapNullKeyLiteral);

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
                schemaRegistryClient = SchemaRegistryClientFactory.withNamespace(getDefaultDatabase(), config);
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

        if (schemaRegistryClient != null) {
            try {
                schemaRegistryClient.close();
                log.info("Close connection to Pravega Schema registry");
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

        log.info("aaa: {}", databaseList);
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
            streamManager.deleteScope(name, cascade);
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
                .map(s -> s.getStreamName())
                .filter(s -> !s.startsWith("_"))
                .filter(s -> groupSet.contains(s))
                .collect(Collectors.toList());
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

        String scope = tablePath.getDatabaseName();
        String stream = tablePath.getObjectName();
        changeRegistryNamespace(scope);
        SchemaInfo schemaInfo = schemaRegistryClient.getLatestSchemaVersion(stream, null).getSchemaInfo();
        TableSchema tableSchema = PravegaSchemaUtils.schemaInfoToTableSchema(schemaInfo);

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

        return new CatalogTableImpl(tableSchema, properties, "");
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

        SchemaInfo schemaInfo = PravegaSchemaUtils.tableSchemaToSchemaInfo(table.getSchema(), serializationFormat);
        schemaRegistryClient.addSchema(stream, schemaInfo);
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    // ------ partitions/functions/statistics NOT supported ------

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

    private void changeRegistryNamespace(String namespace) {
        // TODO: If the client keeps the same namespace, it is redundant.
        try {
            schemaRegistryClient.close();
        } catch (Exception e) {
            throw new CatalogException("Fail to close connection to Pravega Schema registry", e);
        }
        schemaRegistryClient = SchemaRegistryClientFactory.withNamespace(namespace, config);
    }

    // put Json related options to properties
    private void propagateJsonOptions(String failOnMissingField, String ignoreParseErrors,
                                      String timestampFormat, String mapNullKeyMode, String mapNullKeyLiteral) {
        if (failOnMissingField != null) {
            properties.put(String.format("%s.%s",
                    PravegaRegistryFormatFactory.IDENTIFIER, JsonOptions.FAIL_ON_MISSING_FIELD.key()),
                    failOnMissingField);
        }
        if (ignoreParseErrors != null) {
            properties.put(String.format("%s.%s",
                    PravegaRegistryFormatFactory.IDENTIFIER, JsonOptions.IGNORE_PARSE_ERRORS.key()),
                    ignoreParseErrors);
        }
        if (timestampFormat != null) {
            properties.put(String.format("%s.%s",
                    PravegaRegistryFormatFactory.IDENTIFIER, JsonOptions.TIMESTAMP_FORMAT.key()),
                    timestampFormat);
        }
        if (mapNullKeyMode != null) {
            properties.put(String.format("%s.%s",
                    PravegaRegistryFormatFactory.IDENTIFIER, JsonOptions.MAP_NULL_KEY_MODE.key()),
                    mapNullKeyMode);
        }
        if (mapNullKeyLiteral != null) {
            properties.put(String.format("%s.%s",
                    PravegaRegistryFormatFactory.IDENTIFIER, JsonOptions.MAP_NULL_KEY_LITERAL.key()),
                    mapNullKeyLiteral);
        }
    }
}
