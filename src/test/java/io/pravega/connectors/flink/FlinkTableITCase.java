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
import io.pravega.connectors.flink.utils.SetupUtils;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.descriptors.Avro;
import org.apache.flink.table.descriptors.ConnectTableDescriptor;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.factories.BatchTableSinkFactory;
import org.apache.flink.table.factories.BatchTableSourceFactory;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Integration tests for {@link FlinkPravegaTableSource} and {@link FlinkPravegaTableSink}.
 */
@Slf4j
public class FlinkTableITCase {

    /**
     * Sample data.
     */
    private static final List<SampleRecord> SAMPLES = Arrays.asList(
            new SampleRecord("A", 1), new SampleRecord("A", 2), new SampleRecord("A", 3),
            new SampleRecord("B", 1), new SampleRecord("B", 2), new SampleRecord("B", 3)
    );

    /**
     * A sample POJO to be written as a Row (category,value).
     */
    @Data
    public static class SampleRecord implements Serializable {
        public String category;
        public int value;

        public SampleRecord() {}

        public SampleRecord(String category, int value) {
            this.category = category;
            this.value = value;
        }
    }

    /**
     * A sample POJO to be written as a Row (category,value,timestamp).
     */
    @Data
    public static class SampleRecordWithTimestamp implements Serializable {
        public String category;
        public int value;
        public long timestamp;

        public SampleRecordWithTimestamp() {}

        public SampleRecordWithTimestamp(SampleRecord record) {
            this.category = record.category;
            this.value = record.value;
            this.timestamp = record.value;
        }
    }

    // The relational schema associated with SampleRecord:
    // root
    //  |-- category: String
    //  |-- value: Integer
    private static final TableSchema SAMPLE_SCHEMA = TableSchema.fromTypeInfo(TypeInformation.of(SampleRecord.class));

    // Ensure each test completes within 120 seconds.
    @Rule
    public Timeout globalTimeout = new Timeout(120, TimeUnit.SECONDS);

    // Setup utility.
    private SetupUtils setupUtils = new SetupUtils();

    @Before
    public void setup() throws Exception {
        this.setupUtils.startAllServices();
    }

    @After
    public void tearDown() throws Exception {
        this.setupUtils.stopAllServices();
    }

    /**
     * Tests the end-to-end functionality of a streaming table source and sink.
     *
     * <p>This test uses the {@link FlinkPravegaTableSink} to emit an in-memory table
     * containing sample data as a Pravega stream of 'append' events (i.e. as a changelog).
     * The test then uses the {@link FlinkPravegaTableSource} to absorb the changelog as a new table.
     *
     * <p>Flink's ability to convert POJOs (e.g. {@link SampleRecord}) to/from table rows is also demonstrated.
     *
     * <p>Because the source is unbounded, the test must throw an exception to deliberately terminate the job.
     *
     * @throws Exception on exception
     */
    @Test
    public void testStreamingTable() throws Exception {

        // create a Pravega stream for test purposes
        Stream stream = Stream.of(setupUtils.getScope(), "FlinkTableITCase.testEndToEnd");
        this.setupUtils.createTestStream(stream.getStreamName(), 1);

        // create a Flink Table environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment().setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance()
                        // watermark is only supported in blink planner
                        .useBlinkPlanner()
                        .inStreamingMode()
                        .build());

        // define a table of sample data from a collection of POJOs.  Schema:
        // root
        //  |-- category: String
        //  |-- value: Integer
        Table table = tableEnv.fromDataStream(env.fromCollection(SAMPLES));

        // write the table to a Pravega stream (using the 'category' column as a routing key)
        FlinkPravegaTableSink sink = FlinkPravegaJsonTableSink.builder()
                .forStream(stream)
                .withPravegaConfig(this.setupUtils.getPravegaConfig())
                .withRoutingKeyField("category")
                .build()
                .configure(SAMPLE_SCHEMA.getFieldNames(), SAMPLE_SCHEMA.getFieldTypes());

        String tableSinkPath = tableEnv.getCurrentDatabase() + "." + "PravegaSink";

        ConnectorCatalogTable<?, ?> connectorCatalogSinkTable = ConnectorCatalogTable.sink(sink, false);

        tableEnv.getCatalog(tableEnv.getCurrentCatalog()).get().createTable(
                ObjectPath.fromString(tableSinkPath),
                connectorCatalogSinkTable, false);

        table.insertInto("PravegaSink");

        // register the Pravega stream as a table called 'samples'
        FlinkPravegaTableSource source = FlinkPravegaJsonTableSource.builder()
                .forStream(stream)
                .withPravegaConfig(this.setupUtils.getPravegaConfig())
                .withSchema(SAMPLE_SCHEMA)
                .build();

        String tableSourcePath = tableEnv.getCurrentDatabase() + "." + "samples";

        ConnectorCatalogTable<?, ?> connectorCatalogSourceTable = ConnectorCatalogTable.source(source, false);

        tableEnv.getCatalog(tableEnv.getCurrentCatalog()).get().createTable(
                ObjectPath.fromString(tableSourcePath),
                connectorCatalogSourceTable, false);

        // select some sample data from the Pravega-backed table, as a view
        Table view = tableEnv.sqlQuery("SELECT * FROM samples WHERE category IN ('A','B')");

        // write the view to a test sink that verifies the data for test purposes
        tableEnv.toAppendStream(view, SampleRecord.class).addSink(new TestSink(SAMPLES));

        // execute the topology
        try {
            env.execute();
            Assert.fail("expected an exception");
        } catch (Exception e) {
            // we expect the job to fail because the test sink throws a deliberate exception.
            Assert.assertTrue(ExceptionUtils.getRootCause(e) instanceof TestCompletionException);
        }
    }


    /**
     * Tests the end-to-end functionality of a batch table source and sink.
     *
     * <p>This test uses the {@link FlinkPravegaTableSink} to emit an in-memory table
     * containing sample data as a Pravega stream of 'append' events (i.e. as a changelog).
     * The test then uses the {@link FlinkPravegaTableSource} to absorb the changelog as a new table.
     *
     * <p>Flink's ability to convert POJOs (e.g. {@link SampleRecord}) to/from table rows is also demonstrated.
     *
     * <p>Because the source is unbounded, the test must throw an exception to deliberately terminate the job.
     *
     * @throws Exception on exception
     */
    @Test
    public void testBatchTable() throws Exception {

        // create a Pravega stream for test purposes
        Stream stream = Stream.of(setupUtils.getScope(), "FlinkTableITCase.testEndToEnd");
        this.setupUtils.createTestStream(stream.getStreamName(), 1);

        // create a Flink Table environment
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        // define a table of sample data from a collection of POJOs.  Schema:
        // root
        //  |-- category: String
        //  |-- value: Integer
        Table table = tableEnv.fromDataSet(env.fromCollection(SAMPLES));

        // write the table to a Pravega stream (using the 'category' column as a routing key)
        FlinkPravegaTableSink sink = FlinkPravegaJsonTableSink.builder()
                .forStream(stream)
                .withPravegaConfig(this.setupUtils.getPravegaConfig())
                .withRoutingKeyField("category")
                .build()
                .configure(SAMPLE_SCHEMA.getFieldNames(), SAMPLE_SCHEMA.getFieldTypes());

        tableEnv.registerTableSink("PravegaSink", sink);
        table.insertInto("PravegaSink");
        env.execute();

        // register the Pravega stream as a table called 'samples'
        FlinkPravegaTableSource source = FlinkPravegaJsonTableSource.builder()
                .forStream(stream)
                .withPravegaConfig(this.setupUtils.getPravegaConfig())
                .withSchema(SAMPLE_SCHEMA)
                .build();
        tableEnv.registerTableSource("samples", source);

        // select some sample data from the Pravega-backed table, as a view
        Table view = tableEnv.sqlQuery("SELECT * FROM samples WHERE category IN ('A','B')");

        // convert the view to a dataset and collect the results for comparison purposes
        List<SampleRecord> results = tableEnv.toDataSet(view, SampleRecord.class).collect();
        Assert.assertEquals(new HashSet<>(SAMPLES), new HashSet<>(results));
    }

    /**
     * Validates the use of Pravega Table Descriptor to generate the source/sink Table factory to
     * write and read from Pravega stream using {@link StreamTableEnvironment}
     * @throws Exception
     */
    @Test
    public void testStreamingTableUsingDescriptor() throws Exception {

        final String scope = setupUtils.getScope();
        final String streamName = "stream";
        Stream stream = Stream.of(scope, streamName);
        this.setupUtils.createTestStream(stream.getStreamName(), 1);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment().setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance()
                        // watermark is only supported in blink planner
                        .useBlinkPlanner()
                        .inStreamingMode()
                        .build());

        PravegaConfig pravegaConfig = setupUtils.getPravegaConfig();

        Pravega pravega = new Pravega();
        pravega.tableSinkWriterBuilder()
                .withRoutingKeyField("category")
                .forStream(stream)
                .withPravegaConfig(pravegaConfig);
        pravega.tableSourceReaderBuilder()
                .withReaderGroupScope(stream.getScope())
                .forStream(stream)
                .withPravegaConfig(pravegaConfig);

        TableSchema tableSchema = TableSchema.builder()
                .field("category", DataTypes.STRING())
                .field("value", DataTypes.INT())
                .build();

        Schema schema = new Schema().schema(tableSchema);

        ConnectTableDescriptor desc = tableEnv.connect(pravega)
                .withFormat(
                    new Json()
                            .failOnMissingField(false)
                )
                .withSchema(schema)
                .inAppendMode();

        desc.createTemporaryTable("test");

        final Map<String, String> propertiesMap = desc.toProperties();
        final TableSink<?> sink = TableFactoryService.find(StreamTableSinkFactory.class, propertiesMap)
                .createStreamTableSink(propertiesMap);
        final TableSource<?> source = TableFactoryService.find(StreamTableSourceFactory.class, propertiesMap)
                .createStreamTableSource(propertiesMap);

        Table table = tableEnv.fromDataStream(env.fromCollection(SAMPLES));

        String tablePathSink = tableEnv.getCurrentDatabase() + "." + "PravegaSink";

        ConnectorCatalogTable<?, ?> connectorCatalogSinkTable = ConnectorCatalogTable.sink(sink, false);

        tableEnv.getCatalog(tableEnv.getCurrentCatalog())
                .get()
                .createTable(
                ObjectPath.fromString(tablePathSink),
                connectorCatalogSinkTable, false);

        table.insertInto("PravegaSink");

        ConnectorCatalogTable<?, ?> connectorCatalogSourceTable = ConnectorCatalogTable.source(source, false);
        String tablePathSource = tableEnv.getCurrentDatabase() + "." + "samples";

        tableEnv.getCatalog(tableEnv.getCurrentCatalog()).get().createTable(
                ObjectPath.fromString(tablePathSource),
                connectorCatalogSourceTable, false);
        // select some sample data from the Pravega-backed table, as a view
        Table view = tableEnv.sqlQuery("SELECT * FROM samples WHERE category IN ('A','B')");

        // write the view to a test sink that verifies the data for test purposes
        tableEnv.toAppendStream(view, SampleRecord.class).addSink(new TestSink(SAMPLES));

        // execute the topology
        try {
            env.execute();
            Assert.fail("expected an exception");
        } catch (Exception e) {
            // we expect the job to fail because the test sink throws a deliberate exception.
            Assert.assertTrue(ExceptionUtils.getRootCause(e) instanceof TestCompletionException);
        }
    }

    /**
     * Validates the use of Pravega Table Descriptor to generate the source/sink Table factory to
     * write and read from Pravega stream using {@link BatchTableEnvironment}
     * @throws Exception
     */
    @Test
    public void testBatchTableUsingDescriptor() throws Exception {

        final String scope = setupUtils.getScope();
        final String streamName = "stream";
        Stream stream = Stream.of(scope, streamName);
        this.setupUtils.createTestStream(stream.getStreamName(), 1);

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        PravegaConfig pravegaConfig = setupUtils.getPravegaConfig();

        Pravega pravega = new Pravega();
        pravega.tableSinkWriterBuilder()
                .withRoutingKeyField("category")
                .forStream(stream)
                .withPravegaConfig(pravegaConfig);
        pravega.tableSourceReaderBuilder()
                .withReaderGroupScope(stream.getScope())
                .forStream(stream)
                .withPravegaConfig(pravegaConfig);

        ConnectTableDescriptor desc = tableEnv.connect(pravega)
                .withFormat(new Json().failOnMissingField(false))
                .withSchema(new Schema().
                        field("category", DataTypes.STRING()).
                        field("value", DataTypes.INT()));
        desc.createTemporaryTable("test");

        final Map<String, String> propertiesMap = desc.toProperties();
        final TableSink<?> sink = TableFactoryService.find(BatchTableSinkFactory.class, propertiesMap)
                .createBatchTableSink(propertiesMap);
        final TableSource<?> source = TableFactoryService.find(BatchTableSourceFactory.class, propertiesMap)
                .createBatchTableSource(propertiesMap);

        Table table = tableEnv.fromDataSet(env.fromCollection(SAMPLES));

        String tableSinkPath = tableEnv.getCurrentDatabase() + "." + "PravegaSink";

        ConnectorCatalogTable<?, ?> connectorCatalogTableSink = ConnectorCatalogTable.sink(sink, true);

        tableEnv.getCatalog(tableEnv.getCurrentCatalog()).get().createTable(
                ObjectPath.fromString(tableSinkPath),
                connectorCatalogTableSink, false);

        table.insertInto("PravegaSink");
        env.execute();

        String tableSourcePath = tableEnv.getCurrentDatabase() + "." + "samples";

        ConnectorCatalogTable<?, ?> connectorCatalogTableSource = ConnectorCatalogTable.source(source, true);

        tableEnv.getCatalog(tableEnv.getCurrentCatalog()).get().createTable(
                ObjectPath.fromString(tableSourcePath),
                connectorCatalogTableSource, false);

        // select some sample data from the Pravega-backed table, as a view
        Table view = tableEnv.sqlQuery("SELECT * FROM samples WHERE category IN ('A','B')");

        // convert the view to a dataset and collect the results for comparison purposes
        List<SampleRecord> results = tableEnv.toDataSet(view, SampleRecord.class).collect();
        Assert.assertEquals(new HashSet<>(SAMPLES), new HashSet<>(results));
    }

    @Test
    public void testStreamTableSinkUsingDescriptor() throws Exception {

        // create a Pravega stream for test purposes
        Stream stream = Stream.of(setupUtils.getScope(), "testStreamTableSinkUsingDescriptor");
        this.setupUtils.createTestStream(stream.getStreamName(), 1);

        // create a Flink Table environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment().setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance()
                        // watermark is only supported in blink planner
                        .useBlinkPlanner()
                        .inStreamingMode()
                        .build());

        Table table = tableEnv.fromDataStream(env.fromCollection(SAMPLES));

        Pravega pravega = new Pravega();
        pravega.tableSinkWriterBuilder()
                .withRoutingKeyField("category")
                .forStream(stream)
                .withPravegaConfig(setupUtils.getPravegaConfig());

        ConnectTableDescriptor desc = tableEnv.connect(pravega)
                .withFormat(new Json().failOnMissingField(true))
                .withSchema(new Schema().
                        field("category", DataTypes.STRING())
                        .field("value", DataTypes.INT()))
                .inAppendMode();
        desc.createTemporaryTable("test");

        final Map<String, String> propertiesMap = desc.toProperties();
        final TableSink<?> sink = TableFactoryService.find(StreamTableSinkFactory.class, propertiesMap)
                .createStreamTableSink(propertiesMap);

        String tablePath = tableEnv.getCurrentDatabase() + "." + "PravegaSink";

        ConnectorCatalogTable<?, ?> connectorCatalogTable = ConnectorCatalogTable.sink(sink, false);

        tableEnv.getCatalog(tableEnv.getCurrentCatalog()).get().createTable(
                ObjectPath.fromString(tablePath),
                connectorCatalogTable, false);

        table.insertInto("PravegaSink");
        env.execute();
    }

    @Test
    public void testStreamTableSinkUsingDescriptorWithWatermark() throws Exception {
        // create a Pravega stream for test purposes
        Stream stream = Stream.of(setupUtils.getScope(), "testStreamTableSinkUsingDescriptorWithWatermark");
        this.setupUtils.createTestStream(stream.getStreamName(), 1);

        // create a Flink Table environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment().setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance()
                        // watermark is only supported in blink planner
                        .useBlinkPlanner()
                        .inStreamingMode()
                        .build());
        DataStream<SampleRecordWithTimestamp> dataStream = env.fromCollection(SAMPLES)
                .map(SampleRecordWithTimestamp::new)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SampleRecordWithTimestamp>() {
                    @Override
                    public long extractAscendingTimestamp(SampleRecordWithTimestamp sampleRecordWithTimestamp) {
                        return sampleRecordWithTimestamp.getTimestamp();
                    }
                });

        Table table = tableEnv.fromDataStream(dataStream, "category, value, UserActionTime.rowtime");

        Pravega pravega = new Pravega();
        pravega.tableSinkWriterBuilder()
                .withRoutingKeyField("category")
                .enableWatermark(true)
                .forStream(stream)
                .withPravegaConfig(setupUtils.getPravegaConfig());

        TableSchema tableSchema = TableSchema.builder()
                .field("category", DataTypes.STRING())
                .field("value", DataTypes.INT())
                .field("timestamp", DataTypes.TIMESTAMP(3))
                .build();

        Schema schema = new Schema().schema(tableSchema);

        ConnectTableDescriptor desc = tableEnv.connect(pravega)
                .withFormat(new Json().failOnMissingField(true))
                .withSchema(schema)
                .inAppendMode();
        desc.createTemporaryTable("test");

        final Map<String, String> propertiesMap = desc.toProperties();
        final TableSink<?> sink = TableFactoryService.find(StreamTableSinkFactory.class, propertiesMap)
                .createStreamTableSink(propertiesMap);

        String tablePath = tableEnv.getCurrentDatabase() + "." + "PravegaSink";

        ConnectorCatalogTable<?, ?> connectorCatalogTable = ConnectorCatalogTable.sink(sink, false);

        tableEnv.getCatalog(tableEnv.getCurrentCatalog()).get().createTable(
                ObjectPath.fromString(tablePath),
                connectorCatalogTable, false);

        table.insertInto(tablePath);
        env.execute();
    }

    @Test
    public void testBatchTableSinkUsingDescriptor() throws Exception {

        // create a Pravega stream for test purposes
        Stream stream = Stream.of(setupUtils.getScope(), "testBatchTableSinkUsingDescriptor");
        this.setupUtils.createTestStream(stream.getStreamName(), 1);

        // create a Flink Table environment
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        Table table = tableEnv.fromDataSet(env.fromCollection(SAMPLES));

        Pravega pravega = new Pravega();
        pravega.tableSinkWriterBuilder()
                .withRoutingKeyField("category")
                .forStream(stream)
                .withPravegaConfig(setupUtils.getPravegaConfig());

        ConnectTableDescriptor desc = tableEnv.connect(pravega)
                .withFormat(new Json().failOnMissingField(true))
                .withSchema(new Schema().field("category", DataTypes.STRING()).
                        field("value", DataTypes.INT()));
        desc.createTemporaryTable("test");

        final Map<String, String> propertiesMap = desc.toProperties();
        final TableSink<?> sink = TableFactoryService.find(BatchTableSinkFactory.class, propertiesMap)
                .createBatchTableSink(propertiesMap);

        String tableSinkPath = tableEnv.getCurrentDatabase() + "." + "PravegaSink";

        ConnectorCatalogTable<?, ?> connectorCatalogSinkTable = ConnectorCatalogTable.sink(sink, true);

        tableEnv.getCatalog(tableEnv.getCurrentCatalog()).get().createTable(
                ObjectPath.fromString(tableSinkPath),
                connectorCatalogSinkTable, false);
        table.insertInto("PravegaSink");
        env.execute();
    }

    @Test
    public void testStreamTableSinkUsingDescriptorForAvro() throws Exception {

        // create a Pravega stream for test purposes
        Stream stream = Stream.of(setupUtils.getScope(), "testStreamTableSinkUsingDescriptorForAvro");
        this.setupUtils.createTestStream(stream.getStreamName(), 1);

        // create a Flink Table environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment().setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance()
                        // watermark is only supported in blink planner
                        .useBlinkPlanner()
                        .inStreamingMode()
                        .build());

        Table table = tableEnv.fromDataStream(env.fromCollection(SAMPLES));

        Pravega pravega = new Pravega();
        pravega.tableSinkWriterBuilder()
                .withRoutingKeyField("category")
                .forStream(stream)
                .withPravegaConfig(setupUtils.getPravegaConfig());

        Avro avro = new Avro();
        String avroSchema =  "{" +
                "  \"type\": \"record\"," +
                "  \"name\": \"test\"," +
                "  \"fields\" : [" +
                "    {\"name\": \"category\", \"type\": \"string\"}," +
                "    {\"name\": \"value\", \"type\": \"int\"}" +
                "  ]" +
                "}";
        avro.avroSchema(avroSchema);

        ConnectTableDescriptor desc = tableEnv.connect(pravega)
                .withFormat(avro)
                .withSchema(new Schema().field("category", DataTypes.STRING()).
                        field("value", DataTypes.INT()))
                .inAppendMode();
        desc.createTemporaryTable("test");

        final Map<String, String> propertiesMap = desc.toProperties();
        final TableSink<?> sink = TableFactoryService.find(StreamTableSinkFactory.class, propertiesMap)
                .createStreamTableSink(propertiesMap);

        String tablePath = tableEnv.getCurrentDatabase() + "." + "PravegaSink";

        ConnectorCatalogTable<?, ?> connectorCatalogTable = ConnectorCatalogTable.sink(sink, false);

        tableEnv.getCatalog(tableEnv.getCurrentCatalog()).get().createTable(
                ObjectPath.fromString(tablePath),
                connectorCatalogTable, false);

        table.insertInto("PravegaSink");
        env.execute();
    }

    private static class TestSink extends RichSinkFunction<SampleRecord> {
        private final LinkedList<SampleRecord> remainingSamples;

        public TestSink(List<SampleRecord> allSamples) {
            remainingSamples = new LinkedList<>(allSamples);
        }

        @Override
        public void invoke(SampleRecord value, Context context) throws Exception {
            remainingSamples.remove(value);
            log.info("processed: {}", value);

            if (remainingSamples.size() == 0) {
                throw new TestCompletionException();
            }
        }
    }

    private static class TestCompletionException extends RuntimeException {

    }
}
