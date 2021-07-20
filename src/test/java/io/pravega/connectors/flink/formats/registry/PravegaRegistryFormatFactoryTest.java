/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink.formats.registry;

import io.pravega.schemaregistry.contract.data.SerializationFormat;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link PravegaRegistryFormatFactory}. */
public class PravegaRegistryFormatFactoryTest extends TestLogger {

    private static final ResolvedSchema RESOLVED_SCHEMA =
            ResolvedSchema.of(
                    Column.physical("a", DataTypes.STRING()),
                    Column.physical("b", DataTypes.INT()),
                    Column.physical("c", DataTypes.BOOLEAN()));

    private static final RowType ROW_TYPE = (RowType) RESOLVED_SCHEMA.toPhysicalRowDataType().getLogicalType();

    private static final String SCOPE = "test-scope";
    private static final String STREAM = "test-stream";
    private static final URI SCHEMAREGISTRY_URI = URI.create("http://localhost:10092");

    private static final SerializationFormat SERIALIZATIONFORMAT = SerializationFormat.Avro;
    private static final boolean FAIL_ON_MISSING_FIELD = false;
    private static final boolean IGNORE_PARSE_ERRORS = false;
    private static final TimestampFormat TIMESTAMP_FORMAT = TimestampFormat.SQL;
    private static final JsonOptions.MapNullKeyMode MAP_NULL_KEY_MODE =
            JsonOptions.MapNullKeyMode.FAIL;
    private static final String MAP_NULL_KEY_LITERAL = "null";

    @Test
    public void testSeDeSchema() {
        final PravegaRegistryRowDataDeserializationSchema expectedDeser =
                new PravegaRegistryRowDataDeserializationSchema(
                        ROW_TYPE,
                        InternalTypeInfo.of(ROW_TYPE),
                        SCOPE,
                        STREAM,
                        SCHEMAREGISTRY_URI,
                        FAIL_ON_MISSING_FIELD,
                        IGNORE_PARSE_ERRORS,
                        TIMESTAMP_FORMAT);

        final Map<String, String> options = getAllOptions();

        final DynamicTableSource actualSource = createTableSource(options);
        assertTrue(actualSource instanceof TestDynamicTableFactory.DynamicTableSourceMock);
        TestDynamicTableFactory.DynamicTableSourceMock sourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

        DeserializationSchema<RowData> actualDeser =
                sourceMock.valueFormat.createRuntimeDecoder(
                        ScanRuntimeProviderContext.INSTANCE, RESOLVED_SCHEMA.toPhysicalRowDataType());

        assertEquals(expectedDeser, actualDeser);

        final PravegaRegistryRowDataSerializationSchema expectedSer =
                new PravegaRegistryRowDataSerializationSchema(
                        ROW_TYPE,
                        SCOPE,
                        STREAM,
                        SCHEMAREGISTRY_URI,
                        SERIALIZATIONFORMAT,
                        TIMESTAMP_FORMAT,
                        MAP_NULL_KEY_MODE,
                        MAP_NULL_KEY_LITERAL);

        final DynamicTableSink actualSink = createTableSink(options);
        assertTrue(actualSink instanceof TestDynamicTableFactory.DynamicTableSinkMock);
        TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
                (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

        SerializationSchema<RowData> actualSer =
                sinkMock.valueFormat.createRuntimeEncoder(null, RESOLVED_SCHEMA.toPhysicalRowDataType());

        assertEquals(expectedSer, actualSer);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private Map<String, String> getAllOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", "test-connector");
        options.put("target", "MyTarget");

        options.put("format", PravegaRegistryFormatFactory.IDENTIFIER);
        options.put("pravega-registry.uri", "http://localhost:10092");
        options.put("pravega-registry.namespace", SCOPE);
        options.put("pravega-registry.group-id", STREAM);
        options.put("pravega-registry.format", SERIALIZATIONFORMAT.name());
        options.put("pravega-registry.fail-on-missing-field", "false");
        options.put("pravega-registry.ignore-parse-errors", "false");
        options.put("pravega-registry.timestamp-format.standard", "SQL");
        options.put("pravega-registry.map-null-key.mode", "FAIL");
        options.put("pravega-registry.map-null-key.literal", "null");
        return options;
    }

    private static DynamicTableSource createTableSource(Map<String, String> options) {
        CatalogTable table = new CatalogTableImpl(TableSchema.fromResolvedSchema(RESOLVED_SCHEMA), options, "scanTable");
        return FactoryUtil.createTableSource(
                null,
                ObjectIdentifier.of("default", "default", "scanTable"),
                new ResolvedCatalogTable(table, RESOLVED_SCHEMA),
                new Configuration(),
                Thread.currentThread().getContextClassLoader(),
                false);
    }

    private static DynamicTableSink createTableSink(Map<String, String> options) {
        CatalogTable table = new CatalogTableImpl(TableSchema.fromResolvedSchema(RESOLVED_SCHEMA), options, "scanTable");
        return FactoryUtil.createTableSink(
                null,
                ObjectIdentifier.of("default", "default", "scanTable"),
                new ResolvedCatalogTable(table, RESOLVED_SCHEMA),
                new Configuration(),
                Thread.currentThread().getContextClassLoader(),
                false);
    }
}
