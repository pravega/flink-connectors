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
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.DataSet;
import io.pravega.connectors.flink.table.descriptors.Pravega;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.net.URI;
import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class FlinkPravegaTableSinkTest {

    private static final TableSchema TUPLE1 = TableSchema.builder().field("f1", DataTypes.STRING()).build();
    private static final TableSchema TUPLE2 = TableSchema.builder()
                                                .field("f1", DataTypes.STRING())
                                                .field("f2", DataTypes.INT()).build();

    @Test
    @SuppressWarnings("unchecked")
    public void testConfigure() {
        FlinkPravegaWriter<Row> writer = mock(FlinkPravegaWriter.class);
        FlinkPravegaOutputFormat<Row> outputFormat = mock(FlinkPravegaOutputFormat.class);
        FlinkPravegaTableSink tableSinkUnconfigured = new FlinkPravegaTableSink(schema -> writer, schema -> outputFormat, TUPLE1);

        FlinkPravegaTableSink tableSink1 = tableSinkUnconfigured.configure(TUPLE1.getFieldNames(), TUPLE1.getFieldTypes());
        assertNotSame(tableSinkUnconfigured, tableSink1);
        assertEquals(TUPLE1.toRowDataType(), tableSink1.getConsumedDataType());
        assertEquals(TUPLE1, tableSink1.getTableSchema());

        FlinkPravegaTableSink tableSink2 = tableSinkUnconfigured.configure(TUPLE2.getFieldNames(), TUPLE2.getFieldTypes());
        assertNotSame(tableSinkUnconfigured, tableSink2);
        assertEquals(TUPLE2.toRowDataType(), tableSink2.getConsumedDataType());
        assertEquals(TUPLE2, tableSink2.getTableSchema());
    }

    @Test
    public void testEmitDataStream() {
        FlinkPravegaWriter<Row> writer = mock(FlinkPravegaWriter.class);
        FlinkPravegaOutputFormat<Row> outputFormat = mock(FlinkPravegaOutputFormat.class);
        FlinkPravegaTableSink tableSink = new FlinkPravegaTableSink(schema -> writer, schema -> outputFormat, TUPLE1);
        TypeInformation<Row> rowTypeInfo = (TypeInformation<Row>) TypeConversions.fromDataTypeToLegacyInfo(TUPLE1.toRowDataType());
        DataStreamMock dataStream = new DataStreamMock(new StreamExecutionEnvironmentMock(), rowTypeInfo);
        tableSink.consumeDataStream(dataStream);
        assertTrue(FlinkPravegaWriter.class.isAssignableFrom(dataStream.sinkFunction.getClass()));
    }

    @Test
    public void testEmitDataSet() {
        FlinkPravegaWriter<Row> writer = mock(FlinkPravegaWriter.class);
        FlinkPravegaOutputFormat<Row> outputFormat = mock(FlinkPravegaOutputFormat.class);
        FlinkPravegaTableSink tableSink = new FlinkPravegaTableSink(schema -> writer, schema -> outputFormat, TUPLE1);
        DataSet<Row> dataSet = mock(DataSet.class);
        tableSink.consumeDataSet(dataSet);
        verify(dataSet).output(outputFormat);
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

    private static class StreamExecutionEnvironmentMock extends StreamExecutionEnvironment {

        @Override
        public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
            throw new UnsupportedOperationException();
        }
    }

    private static class DataStreamMock extends DataStream<Row> {

        private SinkFunction<?> sinkFunction;

        public DataStreamMock(StreamExecutionEnvironment environment, TypeInformation<Row> outType) {
            super(environment, new TransformationMock("name", outType, 1));
        }

        @Override
        public DataStreamSink<Row> addSink(SinkFunction<Row> sinkFunction) {
            this.sinkFunction = sinkFunction;
            return super.addSink(sinkFunction);
        }
    }

    private static class TransformationMock extends Transformation<Row> {

        public TransformationMock(String name, TypeInformation<Row> outputType, int parallelism) {
            super(name, outputType, parallelism);
        }

        @Override
        public Collection<Transformation<?>> getTransitivePredecessors() {
            return null;
        }
    }
}