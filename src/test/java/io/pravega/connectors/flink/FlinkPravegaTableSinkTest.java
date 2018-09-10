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
import io.pravega.connectors.flink.serialization.JsonRowSerializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.function.Function;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class FlinkPravegaTableSinkTest {

    private static final RowTypeInfo TUPLE1 = new RowTypeInfo(Types.STRING);
    private static final RowTypeInfo TUPLE2 = new RowTypeInfo(Types.STRING, Types.INT);
    private static final SerializationSchema<Row> SERIALIZER1 = new JsonRowSerializationSchema(TUPLE1.getFieldNames());
    private static final Stream STREAM1 = Stream.of("scope-1/stream-1");

    @Test
    @SuppressWarnings("unchecked")
    public void testConfigure() {
        FlinkPravegaWriter<Row> writer = mock(FlinkPravegaWriter.class);
        FlinkPravegaOutputFormat<Row> outputFormat = mock(FlinkPravegaOutputFormat.class);
        FlinkPravegaTableSink tableSinkUnconfigured = new TestableFlinkPravegaTableSink(config -> writer, config -> outputFormat);

        FlinkPravegaTableSink tableSink1 = tableSinkUnconfigured.configure(TUPLE1.getFieldNames(), TUPLE1.getFieldTypes());
        assertNotSame(tableSinkUnconfigured, tableSink1);
        assertEquals(TUPLE1, tableSink1.getOutputType());
        assertArrayEquals(TUPLE1.getFieldNames(), tableSink1.getFieldNames());
        assertArrayEquals(TUPLE1.getFieldTypes(), tableSink1.getFieldTypes());

        FlinkPravegaTableSink tableSink2 = tableSinkUnconfigured.configure(TUPLE2.getFieldNames(), TUPLE2.getFieldTypes());
        assertNotSame(tableSinkUnconfigured, tableSink2);
        assertEquals(TUPLE2, tableSink2.getOutputType());
        assertArrayEquals(TUPLE2.getFieldNames(), tableSink2.getFieldNames());
        assertArrayEquals(TUPLE2.getFieldTypes(), tableSink2.getFieldTypes());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testEmitDataStream() {
        FlinkPravegaWriter<Row> writer = mock(FlinkPravegaWriter.class);
        FlinkPravegaOutputFormat<Row> outputFormat = mock(FlinkPravegaOutputFormat.class);
        FlinkPravegaTableSink tableSink = new TestableFlinkPravegaTableSink(config -> writer, config -> outputFormat)
                .configure(TUPLE1.getFieldNames(), TUPLE1.getFieldTypes());
        DataStream<Row> dataStream = mock(DataStream.class);
        tableSink.emitDataStream(dataStream);
        verify(dataStream).addSink(writer);
    }

    @Test
    public void testEmitDataSet() {
        FlinkPravegaWriter<Row> writer = mock(FlinkPravegaWriter.class);
        FlinkPravegaOutputFormat<Row> outputFormat = mock(FlinkPravegaOutputFormat.class);
        FlinkPravegaTableSink tableSink = new TestableFlinkPravegaTableSink(config -> writer, config -> outputFormat)
                .configure(TUPLE1.getFieldNames(), TUPLE1.getFieldTypes());
        DataSet<Row> dataSet = mock(DataSet.class);
        tableSink.emitDataSet(dataSet);
        verify(dataSet).output(outputFormat);
    }

    @Test
    public void testBuilder() {
        FlinkPravegaTableSink.TableSinkConfiguration config =
                new FlinkPravegaTableSink.TableSinkConfiguration(TUPLE1.getFieldNames(), TUPLE1.getFieldTypes());
        TestableFlinkPravegaTableSink.Builder builder = new TestableFlinkPravegaTableSink.Builder()
                .forStream(STREAM1)
                .withRoutingKeyField(TUPLE1.getFieldNames()[0]);
        FlinkPravegaWriter<Row> writer = builder.createSinkFunction(config);
        assertNotNull(writer);
        assertSame(SERIALIZER1, writer.serializationSchema);
        assertEquals(STREAM1, writer.stream);
        assertEquals(0, ((FlinkPravegaTableSink.RowBasedRouter) writer.eventRouter).getKeyIndex());
        FlinkPravegaOutputFormat<Row> outputFormat = builder.createOutputFormat(config);
        assertNotNull(outputFormat);
        assertEquals(SERIALIZER1, outputFormat.getSerializationSchema());
        assertEquals(STREAM1, Stream.of(outputFormat.getScope(), outputFormat.getStream()));
        assertEquals(0, ((FlinkPravegaTableSink.RowBasedRouter) outputFormat.getEventRouter()).getKeyIndex());
    }

    private static class TestableFlinkPravegaTableSink extends FlinkPravegaTableSink {

        protected TestableFlinkPravegaTableSink(Function<TableSinkConfiguration, FlinkPravegaWriter<Row>> writerFactory,
                                                Function<TableSinkConfiguration, FlinkPravegaOutputFormat<Row>> outputFormatFactory) {
            super(writerFactory, outputFormatFactory);
        }

        @Override
        protected FlinkPravegaTableSink createCopy() {
            return new TestableFlinkPravegaTableSink(writerFactory, outputFormatFactory);
        }

        static class Builder extends AbstractTableSinkBuilder<Builder> {
            @Override
            protected Builder builder() {
                return this;
            }

            @Override
            protected SerializationSchema<Row> getSerializationSchema(String[] fieldNames) {
                return SERIALIZER1;
            }
        }

    }
}