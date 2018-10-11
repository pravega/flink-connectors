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
import org.apache.flink.types.Row;
import org.junit.Test;

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
}