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
import io.pravega.connectors.flink.utils.IntegerGeneratingSource;
import io.pravega.connectors.flink.utils.SetupUtils;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Preconditions;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.Optional;

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
                .addSource(new IntegerGeneratingSource(false, EVENT_COUNT_PER_SOURCE));

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
     * Read the test data from the stream.
     * Note: assumes that all data was written with the same routing key ("fixedkey").
     *
     * @param streamName            The test stream name containing the data to be verified.
     * @throws Exception on any errors.
     */
    private List<Integer> readAllEvents(final String streamName) throws Exception {
        Preconditions.checkNotNull(streamName);

        // Read all data from the stream.
        @Cleanup
        EventStreamReader<Integer> consumer = SETUP_UTILS.getIntegerReader(streamName);
        List<Integer> elements = new ArrayList<>();
        while (true) {
            Integer event = consumer.readNextEvent(1000).getEvent();
            if (event == null) {
                log.info("Reached end of stream: " + streamName);
                break;
            }
            elements.add(event);
            log.trace("Stream: " + streamName + ". Read event: " + event);
        }
        return elements;
    }

    /**
     * Tests the {@link FlinkPravegaWriter} in {@code AT_LEAST_ONCE} mode.
     */
    @Test
    public void testAtLeastOnceWriter() throws Exception {
        // set up the stream
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        SETUP_UTILS.createTestStream(streamName, 1);

        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment()
                .setParallelism(1)
                .enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        execEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

        DataStreamSource<Integer> dataStream = execEnv
                .addSource(new IntegerGeneratingSource(true, EVENT_COUNT_PER_SOURCE));

        FlinkPravegaWriter<Integer> pravegaSink = FlinkPravegaWriter.<Integer>builder()
                .forStream(streamName)
                .withPravegaConfig(SETUP_UTILS.getPravegaConfig())
                .withSerializationSchema(new IntSerializer())
                .withEventRouter(event -> "fixedkey")
                .withWriterMode(PravegaWriterMode.ATLEAST_ONCE)
                .build();
        dataStream.addSink(pravegaSink).setParallelism(2);

        execEnv.execute();

        for (; ; ) {
            List<Integer> readElements = readAllEvents(streamName);

            // Now verify that all expected events are present in the stream. Having extra elements are fine since we are
            // testing the at-least-once writer.
            Collections.sort(readElements);
            int actualEventCount = 0;
            for (int i = 0; i < readElements.size(); ) {
                if (readElements.get(i) != actualEventCount) {
                    throw new IllegalStateException("Element: " + actualEventCount + " missing in the stream");
                }

                while (i < readElements.size() && readElements.get(i) == actualEventCount) {
                    i++;
                }
                actualEventCount++;
            }
            if (EVENT_COUNT_PER_SOURCE == actualEventCount) {
                break;
            }
            // A batch read from Pravega may not return events that were recently written.
            // In this case, we simply retry the read portion of this test.
            log.info("Retrying read query. expected={}, actual={}", EVENT_COUNT_PER_SOURCE, actualEventCount);
        }
    }

    /**
     * Tests the {@link FlinkPravegaWriter} in {@code AT_LEAST_ONCE} mode with watermarking.
     */
    @Test
    public void testAtLeastOnceWriterWithWatermark() throws Exception {
        // set up the stream
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        SETUP_UTILS.createTestStream(streamName, 1);

        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment()
                .setParallelism(1)
                .enableCheckpointing(1000, CheckpointingMode.AT_LEAST_ONCE);
        execEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        execEnv.getConfig().setAutoWatermarkInterval(50);

        DataStream<Integer> dataStream = execEnv
                .addSource(new IntegerGeneratingSource(false, EVENT_COUNT_PER_SOURCE))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Integer>() {
                    @Override
                    public long extractAscendingTimestamp(Integer i) {
                        return i;
                    }
                });

        FlinkPravegaWriter<Integer> pravegaSink = FlinkPravegaWriter.<Integer>builder()
                .forStream(streamName)
                .withPravegaConfig(SETUP_UTILS.getPravegaConfig())
                .withSerializationSchema(new IntSerializer())
                .withWriterMode(PravegaWriterMode.ATLEAST_ONCE)
                .enableWatermark(true)
                .build();
        dataStream.addSink(pravegaSink).setParallelism(2);

        execEnv.execute();

        // Wait for the Pravega controller to generate TimeWindow
        Thread.sleep(11000);

        EventStreamReader<Integer> consumer = SETUP_UTILS.getIntegerReader(streamName);
        consumer.readNextEvent(1000);
        TimeWindow timeWindow = consumer.getCurrentTimeWindow(SETUP_UTILS.getStream(streamName));

        // Assert the TimeWindow proceeds
        Assert.assertNotNull(timeWindow.getUpperTimeBound());
        Assert.assertTrue(timeWindow.getUpperTimeBound() > 0);
    }

    /**
     * Tests the {@link FlinkPravegaWriter} in {@code EXACTLY_ONCE} mode.
     */
    @Test
    public void testExactlyOnceWriter() throws Exception {
        int numElements = 10000;

        Function<StreamExecutionEnvironment, Void> updateEnvFunc = (env) -> {
            env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
            return null;
        };

        testHelper(numElements, updateEnvFunc, null, 30);
    }

    /**
     * Tests the {@link FlinkPravegaWriter} in {@code EXACTLY_ONCE} mode with unaligned checkpoint.
     */
    @Test
    public void testExactlyOnceWithUnalignedCheckpointWriter() throws Exception {
        int numElements = 10000;

        Function<StreamExecutionEnvironment, Void> updateEnvFunc = (env) -> {
            env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().enableUnalignedCheckpoints();
            return null;
        };

        testHelper(numElements, updateEnvFunc, null, 30);
    }

    /**
     * Tests the {@link FlinkPravegaWriter} in {@code EXACTLY_ONCE} mode with watermarking.
     */
    @Test
    public void testExactlyOnceWriterWithWatermark() throws Exception {
        int numElements = 2000;

        Function<StreamExecutionEnvironment, Void> updateEnvFunc = (env) -> {
            env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            env.getConfig().setAutoWatermarkInterval(100);
            return null;
        };
        BiFunction<ThrottledIntegerGeneratingSource, FlinkPravegaWriter.Builder<Integer>, Void>
                updateSourceAndWriterFunc = (source, builder) -> {
            source.withWatermarks(20);
            builder.enableWatermark(true);
            return null;
        };

        testHelper(numElements, updateEnvFunc, updateSourceAndWriterFunc, 1000);
    }

    // ----------------------------------------------------------------------------

    private static class IntSerializer implements SerializationSchema<Integer> {

        @Override
        public byte[] serialize(Integer integer) {
            return ByteBuffer.allocate(4).putInt(0, integer).array();
        }
    }

    void testHelper(int numElements,
                    Function<StreamExecutionEnvironment, Void> updateEnvFunc,
                    @Nullable
                            BiFunction<ThrottledIntegerGeneratingSource, FlinkPravegaWriter.Builder<Integer>, Void> updateSourceAndWriterFunc,
                    int waitSeconds) throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        SETUP_UTILS.createTestStream(streamName, 4);

        CountDownLatch latch = new CountDownLatch(2);
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(new Writer(streamName, numElements, latch, updateEnvFunc, updateSourceAndWriterFunc));
        executorService.execute(new Reader(streamName, numElements, latch));

        boolean wait = latch.await(waitSeconds, TimeUnit.SECONDS);
        if (!wait) {
            Assert.fail("Read/Write operations taking more time to complete");
        }
        executorService.shutdown();
        if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
            executorService.shutdownNow();
        }
    }

    class Task {
        final String streamName;
        final int numElements;
        final CountDownLatch latch;

        private Task(String streamName, int numElements, CountDownLatch latch) {
            this.streamName = streamName;
            this.numElements = numElements;
            this.latch = latch;
        }
    }

    class Writer extends Task implements Runnable {
        // update the flink env with specific configuration
        final Function<StreamExecutionEnvironment, Void> updateEnvFunc;

        // update the source and writer with specific configuration
        @Nullable
        final BiFunction<ThrottledIntegerGeneratingSource, FlinkPravegaWriter.Builder<Integer>, Void>
                updateSourceAndWriterFunc;

        private Writer(String streamName,
                       int numElements,
                       CountDownLatch latch,
                       Function<StreamExecutionEnvironment, Void> updateEnvFunc,
                       BiFunction<ThrottledIntegerGeneratingSource, FlinkPravegaWriter.Builder<Integer>, Void>
                               updateSourceAndWriterFunc) {
            super(streamName, numElements, latch);
            this.updateEnvFunc = updateEnvFunc;
            this.updateSourceAndWriterFunc = updateSourceAndWriterFunc;
        }

        @Override
        public void run() {
            // launch the Flink program that writes and has a failure during writing, to
            // make sure that this does not introduce any duplicates
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L));
            updateEnvFunc.apply(env);

            ThrottledIntegerGeneratingSource source = new ThrottledIntegerGeneratingSource(numElements);
            FlinkPravegaWriter.Builder<Integer> builder = FlinkPravegaWriter.<Integer>builder()
                    .forStream(streamName)
                    .withPravegaConfig(SETUP_UTILS.getPravegaConfig())
                    .withSerializationSchema(new IntSerializer())
                    .withEventRouter(event -> "fixedkey")
                    .withWriterMode(PravegaWriterMode.EXACTLY_ONCE)
                    .withTxnLeaseRenewalPeriod(Time.seconds(30));
            Optional.ofNullable(updateSourceAndWriterFunc).ifPresent(func -> func.apply(source, builder));
            FlinkPravegaWriter<Integer> pravegaSink = builder.build();

            env
                    .addSource(source)
                    .map(new FailingMapper<>(numElements / 2))
                    .addSink(pravegaSink).setParallelism(2);

            try {
                env.execute();
            } catch (Exception e) {
                Assert.fail("Error while writing to Pravega");
            } finally {
                latch.countDown();
            }
        }
    }

    class Reader extends Task implements Runnable {
        private Reader(String streamName, int numElements, CountDownLatch latch) {
            super(streamName, numElements, latch);
        }

        @Override
        public void run() {
            // validate the written data - no duplicates within the first numElements events
            try (EventStreamReader<Integer> reader = SETUP_UTILS.getIntegerReader(streamName)) {
                final BitSet duplicateChecker = new BitSet();

                for (int numElementsRemaining = numElements; numElementsRemaining > 0; ) {
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
                latch.countDown();
            }
        }
    }
}
