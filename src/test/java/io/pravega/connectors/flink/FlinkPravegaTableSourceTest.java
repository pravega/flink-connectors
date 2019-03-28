/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink;

import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.serialization.JsonRowDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;

import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.TableDescriptor;
import org.apache.flink.table.descriptors.SchematicDescriptor;
import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.StreamableDescriptor;
import org.apache.flink.table.descriptors.StreamTableDescriptorValidator;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceUtil;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.net.URI;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link FlinkPravegaTableSource} and its builder.
 */
public class FlinkPravegaTableSourceTest {

    private static final Stream SAMPLE_STREAM = Stream.of("scope", "stream");

    private static final TableSchema SAMPLE_SCHEMA = TableSchema.builder()
            .field("category", Types.STRING)
            .field("value", Types.INT)
            .build();

    @Test
    @SuppressWarnings("unchecked")
    public void testStreamTableSource() {
        FlinkPravegaReader<Row> reader = mock(FlinkPravegaReader.class);
        FlinkPravegaInputFormat<Row> inputFormat = mock(FlinkPravegaInputFormat.class);

        TestableFlinkPravegaTableSource tableSource = new TestableFlinkPravegaTableSource(
                () -> reader,
                () -> inputFormat,
                SAMPLE_SCHEMA,
                jsonSchemaToReturnType(SAMPLE_SCHEMA)
        );
        StreamExecutionEnvironment streamEnv = mock(StreamExecutionEnvironment.class);
        tableSource.getDataStream(streamEnv);
        verify(reader).initialize();
        verify(streamEnv).addSource(reader);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testBatchTableSource() {
        FlinkPravegaReader<Row> reader = mock(FlinkPravegaReader.class);
        FlinkPravegaInputFormat<Row> inputFormat = mock(FlinkPravegaInputFormat.class);
        TypeInformation<Row> returnType = jsonSchemaToReturnType(SAMPLE_SCHEMA);

        TestableFlinkPravegaTableSource tableSource = new TestableFlinkPravegaTableSource(
                () -> reader,
                () -> inputFormat,
                SAMPLE_SCHEMA,
                returnType
        );
        ExecutionEnvironment batchEnv = mock(ExecutionEnvironment.class);
        tableSource.getDataSet(batchEnv);
        verify(batchEnv).createInput(inputFormat, returnType);
    }

    @Test
    public void testBuildInputFormat() {
        TestableFlinkPravegaTableSource.TestableBuilder builder = new TestableFlinkPravegaTableSource.TestableBuilder()
                .forStream(SAMPLE_STREAM)
                .withSchema(SAMPLE_SCHEMA);
        assertEquals(SAMPLE_SCHEMA, builder.getTableSchema());
        FlinkPravegaInputFormat<Row> inputFormat = builder.buildInputFormat();
        assertNotNull(inputFormat);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testTableSourceDescriptor() {
        final String cityName = "fruitName";
        final String total = "count";
        final String eventTime = "eventTime";
        final String procTime = "procTime";
        final String controllerUri = "tcp://localhost:9090";
        final long delay = 3000L;
        final String streamName = "test";
        final String scopeName = "test";

        final TableSchema tableSchema = TableSchema.builder()
                .field(cityName, org.apache.flink.table.api.Types.STRING())
                .field(total, org.apache.flink.table.api.Types.DECIMAL())
                .field(eventTime, org.apache.flink.table.api.Types.SQL_TIMESTAMP())
                .field(procTime, org.apache.flink.table.api.Types.SQL_TIMESTAMP())
                .build();

        Stream stream = Stream.of(scopeName, streamName);
        PravegaConfig pravegaConfig = PravegaConfig.fromDefaults()
                .withControllerURI(URI.create(controllerUri))
                .withDefaultScope(scopeName);

        FlinkPravegaJsonTableSource flinkPravegaJsonTableSource = FlinkPravegaJsonTableSource.builder()
                .forStream(stream)
                .withPravegaConfig(pravegaConfig)
                .failOnMissingField(true)
                .withProctimeAttribute(procTime)
                .withRowtimeAttribute(eventTime,
                        new ExistingField(eventTime),
                        new BoundedOutOfOrderTimestamps(delay))
                .withSchema(tableSchema)
                .withReaderGroupScope(stream.getScope())
                .build();

        TableSourceUtil.validateTableSource(flinkPravegaJsonTableSource);

        // construct table source using descriptors and table source factory
        Pravega pravega = new Pravega();
        pravega.tableSourceReaderBuilder()
                .withReaderGroupScope(stream.getScope())
                .forStream(stream)
                .withPravegaConfig(pravegaConfig);

        final TestTableDescriptor testDesc = new TestTableDescriptor(pravega)
                .withFormat(new Json().failOnMissingField(false) .deriveSchema())
                .withSchema(
                        new Schema()
                                .field(cityName, org.apache.flink.table.api.Types.STRING())
                                .field(total, org.apache.flink.table.api.Types.DECIMAL())
                                .field(eventTime, org.apache.flink.table.api.Types.SQL_TIMESTAMP())
                                    .rowtime(new Rowtime()
                                                .timestampsFromField(eventTime)
                                                .watermarksFromStrategy(new BoundedOutOfOrderTimestamps(delay))
                                            )
                                .field(procTime, org.apache.flink.table.api.Types.SQL_TIMESTAMP()).proctime())
                .inAppendMode();

        final Map<String, String> propertiesMap = testDesc.toProperties();
        final TableSource<?> actualSource = TableFactoryService.find(StreamTableSourceFactory.class, propertiesMap)
                .createStreamTableSource(propertiesMap);
        assertNotNull(actualSource);
        TableSourceUtil.validateTableSource(actualSource);
    }

    /** Converts the JSON schema into into the return type. */
    private static RowTypeInfo jsonSchemaToReturnType(TableSchema jsonSchema) {
        return new RowTypeInfo(jsonSchema.getTypes(), jsonSchema.getColumnNames());
    }

    private static class TestableFlinkPravegaTableSource extends FlinkPravegaTableSource {

        protected TestableFlinkPravegaTableSource(Supplier<FlinkPravegaReader<Row>> sourceFunctionFactory, Supplier<FlinkPravegaInputFormat<Row>> inputFormatFactory, TableSchema schema, TypeInformation<Row> returnType) {
            super(sourceFunctionFactory, inputFormatFactory, schema, returnType);
        }

        @Override
        public String explainSource() {
            return "TestableFlinkPravegaTableSource";
        }

        static class TestableBuilder extends FlinkPravegaTableSource.BuilderBase<TestableFlinkPravegaTableSource, TestableBuilder> {

            @Override
            protected TestableBuilder builder() {
                return this;
            }

            @Override
            protected DeserializationSchema<Row> getDeserializationSchema() {
                TableSchema tableSchema = getTableSchema();
                return new JsonRowDeserializationSchema(jsonSchemaToReturnType(tableSchema));
            }
        }
    }

    /**
     * Test Table descriptor wrapper
     */
    static class TestTableDescriptor extends TableDescriptor implements SchematicDescriptor<TestTableDescriptor>, StreamableDescriptor<TestTableDescriptor> {

        private FormatDescriptor formatDescriptor;
        private Schema schemaDescriptor;
        private ConnectorDescriptor connectorDescriptor;
        private String updateMode;

        public TestTableDescriptor(ConnectorDescriptor connectorDescriptor) {
            this.connectorDescriptor = connectorDescriptor;
        }

        @Override
        public TestTableDescriptor withFormat(FormatDescriptor format) {
            this.formatDescriptor = format;
            return this;
        }

        @Override
        public TestTableDescriptor withSchema(Schema schema) {
            this.schemaDescriptor = schema;
            return this;
        }

        @Override
        public TestTableDescriptor inAppendMode() {
            this.updateMode = StreamTableDescriptorValidator.UPDATE_MODE_VALUE_APPEND();
            return this;
        }

        @Override
        public TestTableDescriptor inRetractMode() {
            this.updateMode = StreamTableDescriptorValidator.UPDATE_MODE_VALUE_RETRACT();
            return this;
        }

        @Override
        public TestTableDescriptor inUpsertMode() {
            this.updateMode = StreamTableDescriptorValidator.UPDATE_MODE_VALUE_UPSERT();
            return this;
        }

        @Override
        public Map<String, String> toProperties() {
            final DescriptorProperties properties = new DescriptorProperties();
            if (formatDescriptor != null) {
                properties.putProperties(formatDescriptor.toProperties());
            }
            if (connectorDescriptor != null) {
                properties.putProperties(connectorDescriptor.toProperties());
            }
            if (schemaDescriptor != null) {
                properties.putProperties(schemaDescriptor.toProperties());
            }
            if (updateMode != null) {
                properties.putString(StreamTableDescriptorValidator.UPDATE_MODE(), updateMode);
            }
            return properties.asMap();
        }
    }
}