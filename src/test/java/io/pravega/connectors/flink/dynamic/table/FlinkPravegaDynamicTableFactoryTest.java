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


import io.pravega.connectors.flink.FlinkPravegaReader;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class FlinkPravegaDynamicTableFactoryTest {
    private static final String STREAM = "stream";
    private static final String NAME = "name";
    private static final String COUNT = "count";
    private static final String TIME = "time";
    private static final String WATERMARK_EXPRESSION = TIME + " - INTERVAL '5' SECOND";
    private static final DataType WATERMARK_DATATYPE = DataTypes.TIMESTAMP(3);
    private static final String COMPUTED_COLUMN_NAME = "computed-column";
    private static final String COMPUTED_COLUMN_EXPRESSION = COUNT + " + 1.0";
    private static final DataType COMPUTED_COLUMN_DATATYPE = DataTypes.DECIMAL(10, 3);
    private static final String SEMANTIC = "exactly-once";

    private static final Properties PRAVEGA_SOURCE_PROPERTIES = new Properties();
    private static final Properties PRAVEGA_SINK_PROPERTIES = new Properties();
    static {
        PRAVEGA_SOURCE_PROPERTIES.setProperty("connection.controller-uri", "dummy");
        PRAVEGA_SOURCE_PROPERTIES.setProperty("connection.default-scope", "dummy");
        PRAVEGA_SOURCE_PROPERTIES.setProperty("scan.stream", STREAM);

        PRAVEGA_SINK_PROPERTIES.setProperty("connection.controller-uri", "dummy");
        PRAVEGA_SINK_PROPERTIES.setProperty("connection.default-scope", "dummy");
    }

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

    @Test
    @SuppressWarnings("unchecked")
    public void testTableSource() {
        // prepare parameters for Pravega table source
        final DataType producedDataType = SOURCE_SCHEMA.toPhysicalRowDataType();

        DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                new TestFormatFactory.DecodingFormatMock(",", true);

        // Construct table source using options and table source factory
        ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
                "default",
                "default",
                "scanTable");
        CatalogTable catalogTable = createPravegaSourceCatalogTable();
        final DynamicTableSource actualSource = FactoryUtil.createTableSource(null,
                objectIdentifier,
                catalogTable,
                new Configuration(),
                Thread.currentThread().getContextClassLoader());

        // Test scan source equals
        final FlinkPravegaDynamicTableSource expectedPravegaSource = new FlinkPravegaDynamicTableSource(
                producedDataType,
                topics,
                topicPattern,
                properties,
                decodingFormat,
                startupMode,
                specificStartupOffsets,
                startupTimestamp);

        final FlinkPravegaDynamicTableSource actualPravegaSource = (FlinkPravegaDynamicTableSource) actualSource;
        assertEquals(actualPravegaSource, expectedPravegaSource);

        // Test Pravega consumer
        ScanTableSource.ScanRuntimeProvider provider =
                actualPravegaSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        assertThat(provider, instanceOf(SourceFunctionProvider.class));
        final SourceFunctionProvider sourceFunctionProvider = (SourceFunctionProvider) provider;
        final SourceFunction<RowData> sourceFunction = sourceFunctionProvider.createSourceFunction();
        assertThat(sourceFunction, instanceOf(FlinkPravegaReader.class));
        //  Test commitOnCheckpoints flag should be true when set consumer group
        assertTrue(((FlinkPravegaConsumerBase) sourceFunction).getEnableCommitOnCheckpoints());
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

        final DynamicTableSink expectedSink = getExpectedSink(
                consumedDataType,
                TOPIC,
                Pravega_SINK_PROPERTIES,
                Optional.of(new FlinkFixedPartitioner<>()),
                encodingFormat,
                PravegaSinkSemantic.EXACTLY_ONCE
        );
        assertEquals(expectedSink, actualSink);

        // Test sink format.
        final PravegaDynamicSinkBase actualPravegaSink = (PravegaDynamicSinkBase) actualSink;
        assertEquals(encodingFormat, actualPravegaSink.encodingFormat);

        // Test Pravega producer.
        DynamicTableSink.SinkRuntimeProvider provider =
                actualPravegaSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        assertThat(provider, instanceOf(SinkFunctionProvider.class));
        final SinkFunctionProvider sinkFunctionProvider = (SinkFunctionProvider) provider;
        final SinkFunction<RowData> sinkFunction = sinkFunctionProvider.createSinkFunction();
        assertThat(sinkFunction, instanceOf(getExpectedProducerClass()));
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    private CatalogTable createPravegaSourceCatalogTable() {
        return createPravegaSourceCatalogTable(getFullSourceOptions());
    }

    private CatalogTable createPravegaSinkCatalogTable() {
        return createPravegaSinkCatalogTable(getFullSinkOptions());
    }

    private CatalogTable createPravegaSourceCatalogTable(Map<String, String> options) {
        return new CatalogTableImpl(SOURCE_SCHEMA, options, "scanTable");
    }

    protected CatalogTable createPravegaSinkCatalogTable(Map<String, String> options) {
        return new CatalogTableImpl(SINK_SCHEMA, options, "sinkTable");
    }

    protected Map<String, String> getFullSourceOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        // Pravega specific options.
        tableOptions.put("connector", "pravega");
        tableOptions.put("stream", stream);
        tableOptions.put("properties.group.id", "dummy");
        tableOptions.put("properties.bootstrap.servers", "dummy");
        tableOptions.put("scan.startup.mode", "specific-offsets");
        tableOptions.put("scan.startup.specific-offsets", PROPS_SCAN_OFFSETS);
        tableOptions.put("scan.stream-partition-discovery.interval", DISCOVERY_INTERVAL);
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

    protected Map<String, String> getFullSinkOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        // Pravega specific options.
        tableOptions.put("connector", factoryIdentifier());
        tableOptions.put("stream", stream);
        tableOptions.put("properties.group.id", "dummy");
        tableOptions.put("properties.bootstrap.servers", "dummy");
        tableOptions.put("sink.partitioner", PravegaOptions.SINK_PARTITIONER_VALUE_FIXED);
        tableOptions.put("sink.semantic", PravegaOptions.SINK_SEMANTIC_VALUE_EXACTLY_ONCE);
        // Format options.
        tableOptions.put("format", TestFormatFactory.IDENTIFIER);
        final String formatDelimiterKey = String.format("%s.%s",
                TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key());
        tableOptions.put(formatDelimiterKey, ",");
        return tableOptions;
    }
}