/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink.dynamic.table;


import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.PravegaWriterMode;
import io.pravega.connectors.flink.util.FlinkPravegaUtils;
import io.pravega.connectors.flink.util.StreamWithBoundaries;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.TestLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static org.apache.flink.util.CoreMatchers.containsCause;
import static org.junit.Assert.assertEquals;

public class FlinkPravegaDynamicTableFactoryTest extends TestLogger {
    private static final String SCOPE = "scope";
    private static final String CONTROLLER_URI = "dummy";
    private static final String AUTH_TYPE = "basic";
    private static final String AUTH_TOKEN = "token";

    private static final String READER_GROUP = "group";
    private static final String STREAM1 = "stream1";
    private static final String STREAM2 = "stream2";
    private static final String STREAM3 = "stream3";

    private static final String NAME = "name";
    private static final String COUNT = "count";
    private static final String TIME = "time";
    private static final String WATERMARK_EXPRESSION = TIME + " - INTERVAL '5' SECOND";
    private static final DataType WATERMARK_DATATYPE = DataTypes.TIMESTAMP(3);
    private static final String COMPUTED_COLUMN_NAME = "computed-column";
    private static final String COMPUTED_COLUMN_EXPRESSION = COUNT + " + 1.0";
    private static final DataType COMPUTED_COLUMN_DATATYPE = DataTypes.DECIMAL(10, 3);
    private static final String EXACTLY_ONCE = "exactly-once";
    private static final String TIMEOUT_INTERVAL = "2000 ms";
    private static final String LEASE_RENEWAL_INTERVAL = "1 min";
    private static final long TIMEOUT_MILLIS = 2000L;
    private static final long LEASE_MILLIS = 60000L;

    private static final TableSchema SOURCE_SCHEMA = TableSchema.builder()
            .field(NAME, DataTypes.STRING())
            .field(COUNT, DataTypes.DECIMAL(38, 18))
            .field(TIME, DataTypes.TIMESTAMP(3))
            .field(COMPUTED_COLUMN_NAME, COMPUTED_COLUMN_DATATYPE, COMPUTED_COLUMN_EXPRESSION)
            .watermark(TIME, WATERMARK_EXPRESSION, WATERMARK_DATATYPE)
            .build();

    private static final TableSchema SINK_SCHEMA = TableSchema.builder()
            .field(NAME, DataTypes.STRING())
            .field(COUNT, DataTypes.DECIMAL(38, 18))
            .field(TIME, DataTypes.TIMESTAMP(3))
            .build();
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    @SuppressWarnings("unchecked")
    public void testStreamingTableSource() {
        // prepare parameters for Pravega table source
        final DataType producedDataType = SOURCE_SCHEMA.toPhysicalRowDataType();

        DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                new TestFormatFactory.DecodingFormatMock(",", true);

        // Construct table source using options and table source factory
        ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
                "default",
                "default",
                "scanTable");
        CatalogTable catalogTable = createPravegaStreamingSourceCatalogTable();
        final DynamicTableSource actualSource = FactoryUtil.createTableSource(null,
                objectIdentifier,
                catalogTable,
                new Configuration(),
                Thread.currentThread().getContextClassLoader());

        // Test scan source equals
        final FlinkPravegaDynamicTableSource expectedPravegaSource = new FlinkPravegaDynamicTableSource(
                producedDataType,
                decodingFormat,
                READER_GROUP,
                getTestPravegaConfig(),
                getTestScanStreamList(),
                3000L,
                5000L,
                TIMEOUT_MILLIS,
                3,
                Optional.empty(),
                true,
                false);

        final FlinkPravegaDynamicTableSource actualPravegaSource = (FlinkPravegaDynamicTableSource) actualSource;
        assertEquals(actualPravegaSource, expectedPravegaSource);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testBatchTableSource() {
        // prepare parameters for Pravega table source
        final DataType producedDataType = SOURCE_SCHEMA.toPhysicalRowDataType();

        DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                new TestFormatFactory.DecodingFormatMock(",", true);

        // Construct table source using options and table source factory
        ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
                "default",
                "default",
                "scanTable");
        CatalogTable catalogTable = createPravegaBatchSourceCatalogTable();
        final DynamicTableSource actualSource = FactoryUtil.createTableSource(null,
                objectIdentifier,
                catalogTable,
                new Configuration(),
                Thread.currentThread().getContextClassLoader());

        // Test scan source equals
        final FlinkPravegaDynamicTableSource expectedPravegaSource = new FlinkPravegaDynamicTableSource(
                producedDataType,
                decodingFormat,
                READER_GROUP,
                getTestPravegaConfig(),
                getTestScanStreamList(),
                3000L,
                5000L,
                TIMEOUT_MILLIS,
                3,
                Optional.empty(),
                false,
                false);

        final FlinkPravegaDynamicTableSource actualPravegaSource = (FlinkPravegaDynamicTableSource) actualSource;
        assertEquals(actualPravegaSource, expectedPravegaSource);
    }

    @Test
    public void testTableSink() {
        final DataType consumedDataType = SINK_SCHEMA.toPhysicalRowDataType();
        EncodingFormat<SerializationSchema<RowData>> encodingFormat =
                new TestFormatFactory.EncodingFormatMock(",");

        // Construct table sink using options and table sink factory.
        ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
                "default",
                "default",
                "sinkTable");
        final CatalogTable sinkTable = createPravegaSinkCatalogTable();
        final DynamicTableSink actualSink = FactoryUtil.createTableSink(
                null,
                objectIdentifier,
                sinkTable,
                new Configuration(),
                Thread.currentThread().getContextClassLoader());

        final FlinkPravegaDynamicTableSink expectedSink = new FlinkPravegaDynamicTableSink(
                TableSchemaUtils.getPhysicalSchema(SINK_SCHEMA),
                encodingFormat,
                getTestPravegaConfig(),
                Stream.of(SCOPE, STREAM3),
                PravegaWriterMode.EXACTLY_ONCE,
                LEASE_MILLIS,
                false,
                Optional.of(NAME)
        );
        assertEquals(expectedSink, actualSink);
    }

    // --------------------------------------------------------------------------------------------
    // Negative tests
    // --------------------------------------------------------------------------------------------
    @Test
    public void testInvalidScanExecutionType() {
        // Construct table source using DDL and table source factory
        ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
                "default",
                "default",
                "scanTable");
        final Map<String, String> modifiedOptions = getModifiedOptions(
                getFullStreamingSourceOptions(),
                options -> {
                    options.put("scan.execution.type", "abc");
                });
        CatalogTable catalogTable = createPravegaSourceCatalogTable(modifiedOptions);

        thrown.expect(ValidationException.class);
        thrown.expect(containsCause(new ValidationException("Unsupported value 'abc' for 'scan.execution.type'. "
                + "Supported values are ['streaming', 'batch'].")));
        FactoryUtil.createTableSource(null,
                objectIdentifier,
                catalogTable,
                new Configuration(),
                Thread.currentThread().getContextClassLoader());
    }

    @Test
    public void testMissingReaderGroupName() {
        // Construct table source using DDL and table source factory
        ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
                "default",
                "default",
                "scanTable");
        final Map<String, String> modifiedOptions = getModifiedOptions(
                getFullStreamingSourceOptions(),
                options -> {
                    options.remove("scan.reader-group.name");
                });
        CatalogTable catalogTable = createPravegaSourceCatalogTable(modifiedOptions);

        thrown.expect(ValidationException.class);
        thrown.expect(containsCause(new ValidationException("'scan.reader-group.name' is required but missing")));
        FactoryUtil.createTableSource(null,
                objectIdentifier,
                catalogTable,
                new Configuration(),
                Thread.currentThread().getContextClassLoader());
    }

    @Test
    public void testNegativeMaxCheckpointRequest() {
        // Construct table source using DDL and table source factory
        ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
                "default",
                "default",
                "scanTable");
        final Map<String, String> modifiedOptions = getModifiedOptions(
                getFullStreamingSourceOptions(),
                options -> {
                    options.put("scan.reader-group.max-outstanding-checkpoint-request", "-1");
                });
        CatalogTable catalogTable = createPravegaSourceCatalogTable(modifiedOptions);

        thrown.expect(ValidationException.class);
        thrown.expect(containsCause(new ValidationException("'scan.reader-group.max-outstanding-checkpoint-request'" +
                " requires a positive integer, received -1")));
        FactoryUtil.createTableSource(null,
                objectIdentifier,
                catalogTable,
                new Configuration(),
                Thread.currentThread().getContextClassLoader());
    }

    @Test
    public void testMissingSourceStream() {
        // Construct table source using DDL and table source factory
        ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
                "default",
                "default",
                "scanTable");
        final Map<String, String> modifiedOptions = getModifiedOptions(
                getFullStreamingSourceOptions(),
                options -> {
                    options.remove("scan.streams");
                });
        CatalogTable catalogTable = createPravegaSourceCatalogTable(modifiedOptions);

        thrown.expect(ValidationException.class);
        thrown.expect(containsCause(new ValidationException("'scan.streams' is required but missing")));
        FactoryUtil.createTableSource(null,
                objectIdentifier,
                catalogTable,
                new Configuration(),
                Thread.currentThread().getContextClassLoader());
    }

    @Test
    public void testInvalidStartStreamCuts() {
        // Construct table source using DDL and table source factory
        ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
                "default",
                "default",
                "scanTable");
        final Map<String, String> modifiedOptions = getModifiedOptions(
                getFullStreamingSourceOptions(),
                options -> {
                    options.put("scan.start-streamcuts", "abc");
                    options.put("scan.end-streamcuts", "abc;def");
                });
        CatalogTable catalogTable = createPravegaSourceCatalogTable(modifiedOptions);

        thrown.expect(ValidationException.class);
        thrown.expect(containsCause(new ValidationException("Start stream cuts are not matching the number of streams," +
                " having 1, expected 2")));
        FactoryUtil.createTableSource(null,
                objectIdentifier,
                catalogTable,
                new Configuration(),
                Thread.currentThread().getContextClassLoader());
    }

    @Test
    public void testInvalidEndStreamCuts() {
        // Construct table source using DDL and table source factory
        ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
                "default",
                "default",
                "scanTable");
        final Map<String, String> modifiedOptions = getModifiedOptions(
                getFullStreamingSourceOptions(),
                options -> {
                    options.put("scan.start-streamcuts", "abc;def");
                    options.put("scan.end-streamcuts", "abc");
                });
        CatalogTable catalogTable = createPravegaSourceCatalogTable(modifiedOptions);

        thrown.expect(ValidationException.class);
        thrown.expect(containsCause(new ValidationException("End stream cuts are not matching the number of streams," +
                " having 1, expected 2")));
        FactoryUtil.createTableSource(null,
                objectIdentifier,
                catalogTable,
                new Configuration(),
                Thread.currentThread().getContextClassLoader());
    }

    @Test
    public void testInvalidSinkSemantic() {
        // Construct table source using DDL and table source factory
        ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
                "default",
                "default",
                "sinkTable");
        final Map<String, String> modifiedOptions = getModifiedOptions(
                getFullSinkOptions(),
                options -> {
                    options.put("sink.semantic", "abc");
                });
        CatalogTable catalogTable = createPravegaSinkCatalogTable(modifiedOptions);

        thrown.expect(ValidationException.class);
        thrown.expect(containsCause(new ValidationException("Unsupported value 'abc' for 'sink.semantic'. "
                + "Supported values are ['at-least-once', 'exactly-once', 'best-effort'].")));
        FactoryUtil.createTableSink(null,
                objectIdentifier,
                catalogTable,
                new Configuration(),
                Thread.currentThread().getContextClassLoader());
    }

    @Test
    public void testMissingSinkStream() {
        // Construct table source using DDL and table source factory
        ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
                "default",
                "default",
                "sinkTable");
        final Map<String, String> modifiedOptions = getModifiedOptions(
                getFullSinkOptions(),
                options -> {
                    options.remove("sink.stream");
                });
        CatalogTable catalogTable = createPravegaSinkCatalogTable(modifiedOptions);

        thrown.expect(ValidationException.class);
        thrown.expect(containsCause(new ValidationException("'sink.stream' is required but missing")));
        FactoryUtil.createTableSink(null,
                objectIdentifier,
                catalogTable,
                new Configuration(),
                Thread.currentThread().getContextClassLoader());
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    private CatalogTable createPravegaStreamingSourceCatalogTable() {
        return createPravegaSourceCatalogTable(getFullStreamingSourceOptions());
    }

    private CatalogTable createPravegaBatchSourceCatalogTable() {
        return createPravegaSourceCatalogTable(getFullBatchSourceOptions());
    }

    private CatalogTable createPravegaSourceCatalogTable(Map<String, String> options) {
        return new CatalogTableImpl(SOURCE_SCHEMA, options, "scanTable");
    }

    private CatalogTable createPravegaSinkCatalogTable() {
        return createPravegaSinkCatalogTable(getFullSinkOptions());
    }

    private CatalogTable createPravegaSinkCatalogTable(Map<String, String> options) {
        return new CatalogTableImpl(SINK_SCHEMA, options, "sinkTable");
    }

    private static Map<String, String> getModifiedOptions(
            Map<String, String> options,
            Consumer<Map<String, String>> optionModifier) {
        optionModifier.accept(options);
        return options;
    }

    private Map<String, String> getFullStreamingSourceOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        // Pravega connection options.
        tableOptions.put("connector", "pravega");
        tableOptions.put("connection.controller-uri", CONTROLLER_URI);
        tableOptions.put("connection.scope", SCOPE);
        tableOptions.put("connection.security.auth-type", AUTH_TYPE);
        tableOptions.put("connection.security.auth-token", AUTH_TOKEN);
        tableOptions.put("connection.security.validate-hostname", "true");

        tableOptions.put("scan.execution.type", "streaming");
        tableOptions.put("scan.reader-group.name", READER_GROUP);
        tableOptions.put("scan.streams", String.format("%s;%s", STREAM1, STREAM2));
        tableOptions.put("scan.event-read.timeout.interval", TIMEOUT_INTERVAL);

        // Format options.
        tableOptions.put("format", TestFormatFactory.IDENTIFIER);
        final String formatDelimiterKey = String.format("%s.%s",
                TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key());
        final String failOnMissingKey = String.format("%s.%s",
                TestFormatFactory.IDENTIFIER, TestFormatFactory.FAIL_ON_MISSING.key());
        tableOptions.put(formatDelimiterKey, ",");
        tableOptions.put(failOnMissingKey, "true");
        return tableOptions;
    }

    private Map<String, String> getFullBatchSourceOptions() {
        return getModifiedOptions(
                getFullStreamingSourceOptions(),
                options -> {
                    options.put("scan.execution.type", "batch");
                }
        );
    }

    private Map<String, String> getFullSinkOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        // Pravega connection options.
        tableOptions.put("connector", "pravega");
        tableOptions.put("connection.controller-uri", CONTROLLER_URI);
        tableOptions.put("connection.scope", SCOPE);
        tableOptions.put("connection.security.auth-type", AUTH_TYPE);
        tableOptions.put("connection.security.auth-token", AUTH_TOKEN);
        tableOptions.put("connection.security.validate-hostname", "true");

        tableOptions.put("sink.stream", STREAM3);
        tableOptions.put("sink.semantic", EXACTLY_ONCE);
        tableOptions.put("sink.txn-lease-renewal.interval", LEASE_RENEWAL_INTERVAL);
        tableOptions.put("sink.routing-key.field.name", NAME);

        // Format options.
        tableOptions.put("format", TestFormatFactory.IDENTIFIER);
        final String formatDelimiterKey = String.format("%s.%s",
                TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key());
        tableOptions.put(formatDelimiterKey, ",");
        return tableOptions;
    }

    private PravegaConfig getTestPravegaConfig() {
        return PravegaConfig
                .fromDefaults()
                .withControllerURI(URI.create(CONTROLLER_URI))
                .withDefaultScope(SCOPE)
                .withHostnameValidation(true)
                .withCredentials(new FlinkPravegaUtils.SimpleCredentials(AUTH_TYPE, AUTH_TOKEN));
    }

    private List<StreamWithBoundaries> getTestScanStreamList() {
        Stream stream1 = Stream.of(SCOPE, STREAM1);
        Stream stream2 = Stream.of(SCOPE, STREAM2);
        return Arrays.asList(
                StreamWithBoundaries.of(stream1, StreamCut.UNBOUNDED, StreamCut.UNBOUNDED),
                StreamWithBoundaries.of(stream2, StreamCut.UNBOUNDED, StreamCut.UNBOUNDED)
        );
    }
}