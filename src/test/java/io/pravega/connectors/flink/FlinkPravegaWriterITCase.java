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

import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.TimeWindow;
import io.pravega.connectors.flink.util.FlinkPravegaUtils;
import io.pravega.connectors.flink.utils.FailingMapper;
import io.pravega.connectors.flink.utils.SetupUtils;
import io.pravega.connectors.flink.utils.ThrottledIntegerGeneratingSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/**
 * Integration tests for {@link FlinkPravegaWriter}.
 */
@Slf4j
public class FlinkPravegaWriterITCase {

    // Setup utility.
    protected static final SetupUtils SETUP_UTILS = new SetupUtils();

    // Number of events to generate for each of the tests.
    private static final int EVENT_COUNT_PER_SOURCE = 20;

    // Ensure each test completes within 120 seconds.
    @Rule
    public Timeout globalTimeout = new Timeout(2400, TimeUnit.SECONDS);

    @BeforeClass
    public static void setup() throws Exception {
        SETUP_UTILS.startAllServices();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        SETUP_UTILS.stopAllServices();
    }

    @Test
    public void testEventTimeOrderedWriter() throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment();

        Stream stream = Stream.of(SETUP_UTILS.getScope(), "testEventTimeOrderedWriter");
        SETUP_UTILS.createTestStream(stream.getStreamName(), 1);

        DataStreamSource<Integer> dataStream = execEnv
                .addSource(new ThrottledIntegerGeneratingSource(EVENT_COUNT_PER_SOURCE));

        FlinkPravegaWriter<Integer> pravegaSink = FlinkPravegaWriter.<Integer>builder()
                .withPravegaConfig(SETUP_UTILS.getPravegaConfig())
                .forStream(stream)
                .withSerializationSchema(new IntSerializer())
                .withEventRouter(event -> "fixedkey")
                .build();

        FlinkPravegaUtils.writeToPravegaInEventTimeOrder(dataStream, pravegaSink, 1);
        Assert.assertNotNull(execEnv.getExecutionPlan());
    }

    /**
     * Tests the {@link FlinkPravegaWriter} in {@code AT_LEAST_ONCE} mode.
     */
    @Test
    public void testAtLeastOnceWriter() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment()
                .setParallelism(1)
                .enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

        FlinkPravegaWriter<Integer> sink = FlinkPravegaWriter.<Integer>builder()
                .forStream(streamName)
                .withPravegaConfig(SETUP_UTILS.getPravegaConfig())
                .withSerializationSchema(new IntSerializer())
                .withEventRouter(event -> "fixedkey")
                .withWriterMode(PravegaWriterMode.ATLEAST_ONCE)
                .build();

        env
                .addSource(new ThrottledIntegerGeneratingSource(EVENT_COUNT_PER_SOURCE))
                .map(new FailingMapper<>(EVENT_COUNT_PER_SOURCE / 2))
                .addSink(sink).setParallelism(2);

        writeAndCheckData(EVENT_COUNT_PER_SOURCE, streamName, env, true, 100);
    }

    /**
     * Tests the {@link FlinkPravegaWriter} in {@code AT_LEAST_ONCE} mode with watermarking.
     */
    @Test
    public void testAtLeastOnceWriterWithWatermark() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        SETUP_UTILS.createTestStream(streamName, 1);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment()
                .setParallelism(1)
                .enableCheckpointing(1000, CheckpointingMode.AT_LEAST_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(50);

        FlinkPravegaWriter<Integer> sink = FlinkPravegaWriter.<Integer>builder()
                .forStream(streamName)
                .withPravegaConfig(SETUP_UTILS.getPravegaConfig())
                .withSerializationSchema(new IntSerializer())
                .withWriterMode(PravegaWriterMode.ATLEAST_ONCE)
                .enableWatermark(true)
                .build();

        env
                .addSource(new ThrottledIntegerGeneratingSource(EVENT_COUNT_PER_SOURCE))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Integer>() {
                    @Override
                    public long extractAscendingTimestamp(Integer i) {
                        return i;
                    }
                })
                .addSink(sink).setParallelism(2);

        writeAndCheckData(EVENT_COUNT_PER_SOURCE, streamName, env, true, 30);
        checkWatermark(streamName);
    }

    /**
     * Tests the {@link FlinkPravegaWriter} in {@code EXACTLY_ONCE} mode.
     */
    @Test
    public void testExactlyOnceWriter() throws Exception {
        int numElements = 10000;
        final String streamName = RandomStringUtils.randomAlphabetic(20);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1)
                .enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L));

        final FlinkPravegaWriter<Integer> sink = FlinkPravegaWriter.<Integer>builder()
                .forStream(streamName)
                .withPravegaConfig(SETUP_UTILS.getPravegaConfig())
                .withSerializationSchema(new IntSerializer())
                .withEventRouter(event -> "fixedkey")
                .withWriterMode(PravegaWriterMode.EXACTLY_ONCE)
                .withTxnLeaseRenewalPeriod(Time.seconds(30))
                .build();

        env
                .addSource(new ThrottledIntegerGeneratingSource(numElements))
                .map(new FailingMapper<>(numElements / 2))
                .addSink(sink).setParallelism(2);

        writeAndCheckData(numElements, streamName, env, false, 30);
    }

    /**
     * Tests the {@link FlinkPravegaWriter} in {@code EXACTLY_ONCE} mode with unaligned checkpoint.
     */
    @Test
    public void testExactlyOnceWithUnalignedCheckpointWriter() throws Exception {
        int numElements = 10000;
        final String streamName = RandomStringUtils.randomAlphabetic(20);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1)
                .enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L));

        final FlinkPravegaWriter<Integer> sink = FlinkPravegaWriter.<Integer>builder()
                .forStream(streamName)
                .withPravegaConfig(SETUP_UTILS.getPravegaConfig())
                .withSerializationSchema(new IntSerializer())
                .withEventRouter(event -> "fixedkey")
                .withWriterMode(PravegaWriterMode.EXACTLY_ONCE)
                .withTxnLeaseRenewalPeriod(Time.seconds(30))
                .build();

        env
                .addSource(new ThrottledIntegerGeneratingSource(numElements))
                .map(new FailingMapper<>(numElements / 2))
                .addSink(sink).setParallelism(2);

        writeAndCheckData(numElements, streamName, env, false, 30);
    }

    /**
     * Tests the {@link FlinkPravegaWriter} in {@code EXACTLY_ONCE} mode with watermarking.
     */
    @Test
    public void testExactlyOnceWriterWithWatermark() throws Exception {
        int numElements = 2000;
        final String streamName = RandomStringUtils.randomAlphabetic(20);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1)
                .enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100);
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L));

        final FlinkPravegaWriter<Integer> sink = FlinkPravegaWriter.<Integer>builder()
                .forStream(streamName)
                .withPravegaConfig(SETUP_UTILS.getPravegaConfig())
                .withSerializationSchema(new IntSerializer())
                .withWriterMode(PravegaWriterMode.EXACTLY_ONCE)
                .withTxnLeaseRenewalPeriod(Time.seconds(30))
                .enableWatermark(true)
                .build();

        env
                .addSource(new ThrottledIntegerGeneratingSource(numElements).withWatermarks(20))
                .map(new FailingMapper<>(numElements / 2))
                .addSink(sink).setParallelism(2);

        writeAndCheckData(numElements, streamName, env, false, 1000);
        checkWatermark(streamName);
    }

    // ----------------------------------------------------------------------------

    private static class IntSerializer implements SerializationSchema<Integer> {

        @Override
        public byte[] serialize(Integer integer) {
            return ByteBuffer.allocate(4).putInt(0, integer).array();
        }
    }

    /**
     * Write some events and check the result concurrently.
     *
     * @param numElements        The number of events written into Pravega stream.
     * @param streamName         The test Pravega stream name.
     * @param env                The Flink environment to be executed, with the job graph configured.
     * @param allowDuplicate     Check data in AT_LEAST_ONCE or EXACTLY_ONCE mode.
     * @param waitSeconds        The maximum time we wait for the checker.
     * @throws Exception on any errors.
     */
    void writeAndCheckData(int numElements,
                           String streamName,
                           StreamExecutionEnvironment env,
                           boolean allowDuplicate,
                           int waitSeconds) throws Exception {
        SETUP_UTILS.createTestStream(streamName, 4);

        // A synchronization aid that allows the program to wait until
        // both writer and checker complete their tasks.
        CountDownLatch latch = new CountDownLatch(2);

        Runnable writeTask = () -> {
            try {
                env.execute();
            } catch (Exception e) {
                Assert.fail("Error while writing to Pravega");
            } finally {
                latch.countDown();
            }
        };

        Runnable checkTask = () -> {
            // Validate the data.
            // 1. Check if all the events are written to the Pravega stream
            // 2. (Optional, controlled by allowDuplicate) Check if there is a duplication
            // 3. Check there is no more events
            try (EventStreamReader<Integer> reader = SETUP_UTILS.getIntegerReader(streamName)) {
                final BitSet duplicateChecker = new BitSet();

                for (int numElementsRemaining = numElements; numElementsRemaining > 0; ) {
                    final EventRead<Integer> eventRead = reader.readNextEvent(1000);
                    final Integer event = eventRead.getEvent();

                    if (event != null) {
                        numElementsRemaining--;
                        if (allowDuplicate) {
                            assertFalse("found a duplicate", duplicateChecker.get(event));
                        }
                        duplicateChecker.set(event);
                    }
                }

                // No more events should be there
                assertNull("too many elements written", reader.readNextEvent(1000).getEvent());

                // Notify that the checker is complete
                latch.countDown();
            }
        };

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(writeTask);
        executorService.execute(checkTask);

        boolean wait = latch.await(waitSeconds, TimeUnit.SECONDS);
        if (!wait) {
            Assert.fail("Read/Write operations taking more time to complete");
        }
        executorService.shutdown();
        if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
            executorService.shutdownNow();
        }
    }

    /**
     * Check the watermark generated by the Pravega.
     *
     * @param streamName         The Pravega stream name.
     * @throws InterruptedException on interruption.
     */
    void checkWatermark(String streamName) throws InterruptedException {
        // Wait 11 seconds for the Pravega controller to generate TimeWindow
        Thread.sleep(11000);

        EventStreamReader<Integer> consumer = SETUP_UTILS.getIntegerReader(streamName);
        consumer.readNextEvent(1000);
        TimeWindow timeWindow = consumer.getCurrentTimeWindow(SETUP_UTILS.getStream(streamName));

        // Assert the TimeWindow proceeds
        Assert.assertNotNull(timeWindow.getUpperTimeBound());
        Assert.assertTrue(timeWindow.getUpperTimeBound() > 0);
    }
}
