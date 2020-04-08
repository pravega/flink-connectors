/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.net.URI;
import java.util.Map;
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

    @Test(expected = NotImplementedException.class)
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
    @SuppressWarnings("unchecked")
    public void testConsumeDataStream() {
        FlinkPravegaWriter<Row> writer = mock(FlinkPravegaWriter.class);
        FlinkPravegaOutputFormat<Row> outputFormat = mock(FlinkPravegaOutputFormat.class);
        FlinkPravegaTableSink tableSink = new TestableFlinkPravegaTableSink(config -> writer, config -> outputFormat)
                .configure(TUPLE1.getFieldNames(), TUPLE1.getFieldTypes());
        DataStream<Row> dataStream = mock(DataStream.class);
        tableSink.consumeDataStream(dataStream);
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

    @Test
    @SuppressWarnings("unchecked")
    public void testTableSinkDescriptor() {
        final String cityName = "fruitName";
        final String total = "count";
        final String eventTime = "eventTime";
        final String procTime = "procTime";
        final String controllerUri = "tcp://localhost:9090";
        final long delay = 3000L;
        final String streamName = "test";
        final String scopeName = "test";

        Stream stream = Stream.of(scopeName, streamName);
        PravegaConfig pravegaConfig = PravegaConfig.fromDefaults()
                .withControllerURI(URI.create(controllerUri))
                .withDefaultScope(scopeName);

        // construct table sink using descriptors and table sink factory
        Pravega pravega = new Pravega();
        pravega.tableSinkWriterBuilder()
                .withRoutingKeyField(cityName)
                .withWriterMode(PravegaWriterMode.EXACTLY_ONCE)
                .enableWatermark(true)
                .forStream(stream)
                .enableMetrics(true)
                .withPravegaConfig(pravegaConfig);

        final FlinkPravegaTableSourceTest.TestTableDescriptor testDesc = new FlinkPravegaTableSourceTest.TestTableDescriptor(pravega)
                .withFormat(new Json().failOnMissingField(false))
                .withSchema(
                        new Schema()
                                .field(cityName, DataTypes.STRING())
                                .field(total, DataTypes.BIGINT())
                                .field(eventTime, DataTypes.TIMESTAMP(3))
                                .rowtime(new Rowtime()
                                        .timestampsFromField(eventTime)
                                        .watermarksFromStrategy(new BoundedOutOfOrderTimestamps(delay))
                                )
                                .field(procTime, DataTypes.TIMESTAMP(3)).proctime()
                )
                .inAppendMode();

        final Map<String, String> propertiesMap = testDesc.toProperties();
        final TableSink<?> sink = TableFactoryService.find(StreamTableSinkFactory.class, propertiesMap)
                .createStreamTableSink(propertiesMap);

        assertNotNull(sink);
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