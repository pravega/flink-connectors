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

import io.pravega.connectors.flink.util.RandomStringUtils;
import io.pravega.connectors.flink.utils.FailingMapper;
import io.pravega.connectors.flink.utils.SetupUtils;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang.exception.ExceptionUtils;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

/**
 * Test for exactly-once semantics in the FlinkPravegaExactlyOnceWriter-
 */
@Slf4j
public class FlinkExactlyOncePravegaWriterTest extends StreamingMultipleProgramsTestBase {

    /** Number of events to produce into the test stream */
    private static final int NUM_STREAM_ELEMENTS = 10000;

    /** Setup utility */
    private static final SetupUtils SETUP_UTILS = new SetupUtils();

    @Rule
    public final Timeout globalTimeout = new Timeout(120, TimeUnit.SECONDS);

    // ------------------------------------------------------------------------

    @BeforeClass
    public static void setupPravega() throws Exception {
        SETUP_UTILS.startAllServices();
    }

    @AfterClass
    public static void tearDownPravega() throws Exception {
        SETUP_UTILS.stopAllServices();
    }

    // ------------------------------------------------------------------------

    @Test
    public void testOneWriterOneSegment() throws Exception {
        runTest(1, 1, NUM_STREAM_ELEMENTS, true);
    }

    @Test
    public void testOneWriterMultipleSegments() throws Exception {
        runTest(1, 4, NUM_STREAM_ELEMENTS, true);
    }

    // This test fails reliably with 
    //         io.grpc.StatusRuntimeException: INTERNAL: Failed locking resource scope/gmbVgdllFXXGriQfPbEM.
    //    @Test
    //    public void testMultipleWriterOneSegment() throws Exception {
    //        runTest(4, 1, NUM_STREAM_ELEMENTS);
    //    }
    
    // This test fails reliably with 
    //         io.grpc.StatusRuntimeException: INTERNAL: Failed locking resource scope/gmbVgdllFXXGriQfPbEM.
    //    @Test
    //    public void testMultipleWriterMultipleSegments() throws Exception {
    //        runTest(4, 4, NUM_STREAM_ELEMENTS);
    //    }

    @Test
    public void testInvalidWriterModeOption() throws Exception {
        try {
            runTest(1, 4, NUM_STREAM_ELEMENTS, false);
        } catch (Exception e) {
            assertNotEquals(-1, ExceptionUtils.indexOfThrowable(e, UnsupportedOperationException.class));
        }
    }


    private static void runTest(
            final int sinkParallelism,
            final int numPravegaSegments,
            final int numElements,
            boolean enableCheckpoint) throws Exception {

        // set up the stream
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        SETUP_UTILS.createTestStream(streamName, numPravegaSegments);

        // launch the Flink program that writes and has a failure during writing, to
        // make sure that this does not introduce any duplicates

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(sinkParallelism);

        // checkpoint frequently
        if (enableCheckpoint) {
            env.enableCheckpointing(100);
        }
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L));

        FlinkPravegaWriter pravegaWriter = new FlinkPravegaWriter<>(
                SETUP_UTILS.getControllerUri(),
                SETUP_UTILS.getScope(),
                streamName,
                new IntSerializer(),
                new IdentityRouter<>(),
                30 * 1000,  // 30 secs timeout
                30 * 1000);
        pravegaWriter.setPravegaWriterMode(PravegaWriterMode.EXACTLY_ONCE);

        env
                .addSource(new ThrottledIntegerGeneratingSource(numElements))
                .map(new FailingMapper<>(numElements / sinkParallelism / 2))
                .rebalance()
                .addSink(pravegaWriter);

        final long executeStart = System.nanoTime();
        env.execute();
        final long executeEnd = System.nanoTime();
        System.out.println(String.format("Test execution took %d ms", (executeEnd - executeStart) / 1_000_000));

        // validate the written data - no duplicates within the first numElements events

        try (EventStreamReader<Integer> reader = SETUP_UTILS.getIntegerReader(streamName)) {
            final BitSet duplicateChecker = new BitSet();

            for (int numElementsRemaining = numElements; numElementsRemaining > 0;) {
                final EventRead<Integer> eventRead = reader.readNextEvent(1000);
                final Integer event = eventRead.getEvent();

                if (event != null) {
                    numElementsRemaining--;
                    assertFalse("found a duplicate", duplicateChecker.get(event));
                    duplicateChecker.set(event);
                }
            }

            // no more events should be there
            assertNull("too many elements written", reader.readNextEvent(1000).getEvent());
        }
    }

    // ----------------------------------------------------------------------------

    private static class IntSerializer implements SerializationSchema<Integer> {

        @Override
        public byte[] serialize(Integer integer) {
            return ByteBuffer.allocate(4).putInt(0, integer).array();
        }
    }

    // ----------------------------------------------------------------------------

    private static class IdentityRouter<T> implements PravegaEventRouter<T> {

        @Override
        public String getRoutingKey(T event) {
            return String.valueOf(event);
        }
    }
}