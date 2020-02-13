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

import io.pravega.client.stream.EventStreamWriter;
import io.pravega.connectors.flink.utils.FailingMapper;
import io.pravega.connectors.flink.utils.IntSequenceExactlyOnceValidator;
import io.pravega.connectors.flink.utils.IntegerDeserializationSchema;
import io.pravega.connectors.flink.utils.NotifyingMapper;
import io.pravega.connectors.flink.utils.SetupUtils;
import io.pravega.connectors.flink.utils.SuccessException;
import io.pravega.connectors.flink.utils.ThrottledIntegerWriter;
import io.pravega.connectors.flink.watermark.LowerBoundAssigner;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.Collector;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeUnit;

/**
 * Integration tests for {@link FlinkPravegaReader}.
 */
@Slf4j
public class FlinkPravegaReaderITCase extends AbstractTestBase {

    // Setup utility.
    protected static final SetupUtils SETUP_UTILS = new SetupUtils();

    // Number of events to produce into the test stream.
    private static final int NUM_STREAM_ELEMENTS = 10000;

    //Ensure each test completes within 180 seconds.
    @Rule
    public final Timeout globalTimeout = new Timeout(180, TimeUnit.SECONDS);

    @BeforeClass
    public static void setupPravega() throws Exception {
        SETUP_UTILS.startAllServices();
    }

    @AfterClass
    public static void tearDownPravega() throws Exception {
        SETUP_UTILS.stopAllServices();
    }

    @Test
    public void testOneSourceOneSegment() throws Exception {
        runTest(1, 1, NUM_STREAM_ELEMENTS);
    }

    @Test
    public void testOneSourceMultipleSegments() throws Exception {
        runTest(1, 4, NUM_STREAM_ELEMENTS);
    }

    // this test currently does ot work, see https://github.com/pravega/pravega/issues/1152
    //@Test
    //public void testMultipleSourcesOneSegment() throws Exception {
    //    runTest(4, 1, NUM_STREAM_ELEMENTS);
    //}

    @Test
    public void testMultipleSourcesMultipleSegments() throws Exception {
        runTest(4, 4, NUM_STREAM_ELEMENTS);
    }

    @Test
    public void testWatermark() throws Exception {
        // set up the stream
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        SETUP_UTILS.createTestStream(streamName, 4);
        try (
                final EventStreamWriter<Integer> eventWriter = SETUP_UTILS.getIntegerWriter(streamName);

                // create the producer that writes to the stream
                final ThrottledIntegerWriter producer = new ThrottledIntegerWriter(
                        eventWriter,
                        NUM_STREAM_ELEMENTS,
                        NUM_STREAM_ELEMENTS + 1,  // no need to block
                        0,                 // the sleep time per element
                        true
                )
        ) {
            producer.start();
            producer.sync();

            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            env.setParallelism(4);
            env.getConfig().setAutoWatermarkInterval(1000);

            // the Pravega reader
            final FlinkPravegaReader<Integer> pravegaSource = FlinkPravegaReader.<Integer>builder()
                    .forStream(streamName)
                    .enableMetrics(false)
                    .withPravegaConfig(SETUP_UTILS.getPravegaConfig())
                    .withDeserializationSchema(new IntegerDeserializationSchema())
                    .withTimestampAssigner(new LowerBoundAssigner<Integer>() {
                        @Override
                        public long extractTimestamp(Integer element, long previousElementTimestamp) {
                            return element;
                        }
                    })
                    .build();

            env
                    .addSource(pravegaSource)
                    .timeWindowAll(Time.seconds(1))
                    .apply(new AllWindowFunction<Integer, Integer, TimeWindow>() {
                        @Override
                        public void apply(TimeWindow window, Iterable<Integer> arr, Collector<Integer> collector) throws Exception {
                            for (Integer i : arr) {
                                collector.collect(i);
                            }
                        }
                    })
                    .addSink(new IntSequenceExactlyOnceValidator(NUM_STREAM_ELEMENTS))
                    .setParallelism(1);

            final long executeStart = System.nanoTime();

            // if these calls complete without exception, then the test passes
            try {
                env.execute();
            } catch (Exception e) {
                if (ExceptionUtils.getRootCause(e) instanceof AssertionError) {
                    throw (AssertionError) ExceptionUtils.getRootCause(e);
                }

                if (!(ExceptionUtils.getRootCause(e) instanceof SuccessException)) {
                    Assert.fail("Unexpected error occurred in the test. " + ExceptionUtils.getRootCauseMessage(e));
                }
            }

            final long executeEnd = System.nanoTime();
            System.out.println(String.format("Test execution took %d ms", (executeEnd - executeStart) / 1_000_000));
        }
    }

    private static void runTest(
            final int sourceParallelism,
            final int numPravegaSegments,
            final int numElements) throws Exception {

        // set up the stream
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        SETUP_UTILS.createTestStream(streamName, numPravegaSegments);

        try (
                final EventStreamWriter<Integer> eventWriter = SETUP_UTILS.getIntegerWriter(streamName);

                // create the producer that writes to the stream
                final ThrottledIntegerWriter producer = new ThrottledIntegerWriter(
                        eventWriter,
                        numElements,
                        numElements / 2,  // the latest when a checkpoint must have happened
                        1,                 // the initial sleep time per element
                        false
                )

        ) {
            producer.start();

            // the producer is throttled so that we don't run the (whatever small) risk of pumping
            // all elements through before completing the first checkpoint (that would make the test senseless)

            // to speed the test up, we un-throttle the producer as soon as the first checkpoint
            // has gone through. Rather than implementing a complicated observer that polls the status
            // from Flink, we simply forward the 'checkpoint complete' notification from the user functions
            // the thr throttler, via a static variable
            NotifyingMapper.TO_CALL_ON_COMPLETION.set(producer::unthrottle);

            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            env.setParallelism(sourceParallelism);
            env.enableCheckpointing(100);
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 0L));

            // we currently need this to work around the case where tasks are
            // started too late, a checkpoint was already triggered, and some tasks
            // never see the checkpoint event
            env.getCheckpointConfig().setCheckpointTimeout(20000);

            // the Pravega reader
            final FlinkPravegaReader<Integer> pravegaSource = FlinkPravegaReader.<Integer>builder()
                    .forStream(streamName)
                    .enableMetrics(false)
                    .withPravegaConfig(SETUP_UTILS.getPravegaConfig())
                    .withDeserializationSchema(new IntegerDeserializationSchema())
                    .build();

            env
                    .addSource(pravegaSource)
                    // this mapper throws an exception at 2/3rd of the data stream,
                    // which is strictly after the checkpoint happened (the latest at 1/2 of the stream)

                    // to make sure that this is not affected by how fast subtasks of the source
                    // manage to pull data from pravega, we make this task non-parallel
                    .map(new FailingMapper<>(numElements * 2 / 3))
                    .setParallelism(1)

                    // hook in the notifying mapper
                    .map(new NotifyingMapper<>())
                    .setParallelism(1)

                    // the sink validates that the exactly-once semantics hold
                    // it must be non-parallel so that it sees all elements and can trivially
                    // check for duplicates
                    .addSink(new IntSequenceExactlyOnceValidator(numElements))
                    .setParallelism(1);

            final long executeStart = System.nanoTime();

            // if these calls complete without exception, then the test passes
            try {
                env.execute();
            } catch (Exception e) {
                if (!(ExceptionUtils.getRootCause(e) instanceof SuccessException)) {
                    throw e;
                }
            }

            // this method forwards exception thrown in the data generator thread
            producer.sync();

            final long executeEnd = System.nanoTime();
            System.out.println(String.format("Test execution took %d ms", (executeEnd - executeStart) / 1_000_000));
        }
    }
}
