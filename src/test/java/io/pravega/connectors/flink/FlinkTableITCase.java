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
import io.pravega.connectors.flink.utils.SetupUtils;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
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
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

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
                .build();
        table.writeToSink(sink);

        // register the Pravega stream as a table called 'samples'
        FlinkPravegaTableSource source = FlinkPravegaJsonTableSource.builder()
                .forStream(stream)
                .withPravegaConfig(this.setupUtils.getPravegaConfig())
                .withSchema(SAMPLE_SCHEMA)
                .build();
        tableEnv.registerTableSource("samples", source);

        // select some sample data from the Pravega-backed table, as a view
        Table view = tableEnv.sqlQuery("SELECT * FROM samples WHERE category IN ('A','B')");

        // write the view to a test sink that verifies the data for test purposes
        tableEnv.toAppendStream(view, SampleRecord.class).addSink(new TestSink(SAMPLES));

        // execute the topology
        try {
            env.execute();
            Assert.fail("expected an exception");
        } catch (JobExecutionException e) {
            // we expect the job to fail because the test sink throws a deliberate exception.
            Assert.assertTrue(e.getCause() instanceof TestCompletionException);
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
    @Ignore("[issue-124] FlinkPravegaTableSink doesn't support BatchTableSink")
    public void testBatchTable() throws Exception {

        // create a Pravega stream for test purposes
        Stream stream = Stream.of(setupUtils.getScope(), "FlinkTableITCase.testEndToEnd");
        this.setupUtils.createTestStream(stream.getStreamName(), 1);

        // create a Flink Table environment
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

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
                .build();
        table.writeToSink(sink);

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
