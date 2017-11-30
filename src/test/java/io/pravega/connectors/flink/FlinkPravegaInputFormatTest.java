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

import io.pravega.client.stream.EventStreamWriter;
import io.pravega.connectors.flink.utils.SetupUtils;
import io.pravega.connectors.flink.utils.ThrottledIntegerWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.streaming.util.serialization.AbstractDeserializationSchema;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public class FlinkPravegaInputFormatTest extends StreamingMultipleProgramsTestBase {

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

    @Test
    public void testBatchInput() throws Exception {
        final int numElements = 100;

        // set up the stream
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        SETUP_UTILS.createTestStream(streamName, 3);

        try (
                final EventStreamWriter<Integer> eventWriter = SETUP_UTILS.getIntegerWriter(streamName);

                // create the producer that writes to the stream
                final ThrottledIntegerWriter producer = new ThrottledIntegerWriter(
                        eventWriter,
                        numElements,
                        numElements + 1, // no need to block writer for a batch test
                        0
                )
        ) {
            // write batch input
            producer.start();
            producer.sync();

            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(3);

            // simple pipeline that reads from Pravega and collects the events
            List<Integer> integers = env.createInput(
                    new FlinkPravegaInputFormat<>(
                            SETUP_UTILS.getControllerUri(),
                            SETUP_UTILS.getScope(),
                            Collections.singleton(streamName),
                            0,
                            new IntDeserializer()),
                    BasicTypeInfo.INT_TYPE_INFO
            ).collect();

            // verify that all events were read
            Assert.assertEquals(numElements, integers.size());
        }
    }

    @Test
    public void testBatchInputWithFailure() throws Exception {
        final int numElements = 100;

        // set up the stream
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        SETUP_UTILS.createTestStream(streamName, 3);

        try (
                final EventStreamWriter<Integer> eventWriter = SETUP_UTILS.getIntegerWriter(streamName);

                // create the producer that writes to the stream
                final ThrottledIntegerWriter producer = new ThrottledIntegerWriter(
                        eventWriter,
                        numElements,
                        numElements + 1, // no need to block writer for a batch test
                        0
                )
        ) {
            // write batch input
            producer.start();
            producer.sync();

            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 1000L));
            env.setParallelism(3);

            // simple pipeline that reads from Pravega and collects the events
            List<Integer> integers = env.createInput(
                    new FlinkPravegaInputFormat<>(
                            SETUP_UTILS.getControllerUri(),
                            SETUP_UTILS.getScope(),
                            Collections.singleton(streamName),
                            0,
                            new IntDeserializer()),
                    BasicTypeInfo.INT_TYPE_INFO
            ).map(new FailOnceMapper(numElements / 2)).collect();

            // verify that the job did fail, and all events were still read
            Assert.assertTrue(FailOnceMapper.hasFailed());
            Assert.assertEquals(numElements, integers.size());

            FailOnceMapper.reset();
        }
    }

    private static class IntDeserializer extends AbstractDeserializationSchema<Integer> {

        @Override
        public Integer deserialize(byte[] message) throws IOException {
            return ByteBuffer.wrap(message).getInt();
        }

        @Override
        public boolean isEndOfStream(Integer nextElement) {
            return false;
        }
    }

    private static class FailOnceMapper extends RichMapFunction<Integer, Integer> {

        private static boolean failedOnce;

        private final int failCount;

        static void reset() {
            failedOnce = false;
        }

        static boolean hasFailed() {
            return failedOnce;
        }

        FailOnceMapper(int failCount) {
            this.failCount = failCount;
        }

        @Override
        public Integer map(Integer value) throws Exception {
            if (!failedOnce && value.equals(failCount)) {
                failedOnce = true;
                throw new RuntimeException("Artificial failure");
            }

            return value;
        }
    }
}