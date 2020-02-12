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
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.TimeWindow;
import io.pravega.connectors.flink.util.FlinkPravegaUtils;
import io.pravega.connectors.flink.utils.FailingMapper;
import io.pravega.connectors.flink.utils.IntegerGeneratingSource;
import io.pravega.connectors.flink.utils.SetupUtils;

import com.google.common.base.Preconditions;
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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
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

        // TODO: Remove the end marker workaround once the following issue is fixed:
        // https://github.com/pravega/pravega/issues/408
        final int streamEndMarker = 99999;

        // Write the end marker.
        @Cleanup
        EventStreamWriter<Integer> eventWriter = SETUP_UTILS.getIntegerWriter(streamName);
        eventWriter.writeEvent("fixedkey", streamEndMarker);
        eventWriter.flush();

        // Read all data from the stream.
        @Cleanup
        EventStreamReader<Integer> consumer = SETUP_UTILS.getIntegerReader(streamName);
        List<Integer> elements = new ArrayList<>();
        while (true) {
            Integer event = consumer.readNextEvent(1000).getEvent();
            if (event == null || event == streamEndMarker) {
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
        List<Integer> readElements = readAllEvents(streamName);

        // Now verify that all expected events are present in the stream. Having extra elements are fine since we are
        // testing the at-least-once writer.
        Collections.sort(readElements);
        int expectedEventValue = 0;
        for (int i = 0; i < readElements.size();) {
            if (readElements.get(i) != expectedEventValue) {
                throw new IllegalStateException("Element: " + expectedEventValue + " missing in the stream");
            }

            while (i < readElements.size() && readElements.get(i) == expectedEventValue) {
                i++;
            }
            expectedEventValue++;
        }
        Assert.assertEquals(expectedEventValue, EVENT_COUNT_PER_SOURCE);
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
                .withEventRouter(event -> "fixedkey")
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

        // set up the stream
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        SETUP_UTILS.createTestStream(streamName, 4);

        CountDownLatch latch = new CountDownLatch(2);

        Runnable writeTask = () -> {
            // launch the Flink program that writes and has a failure during writing, to
            // make sure that this does not introduce any duplicates
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                    .setParallelism(1)
                    .enableCheckpointing(100);
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L));

            FlinkPravegaWriter<Integer> pravegaSink = FlinkPravegaWriter.<Integer>builder()
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
                    .addSink(pravegaSink).setParallelism(2);

            try {
                env.execute();
            } catch (Exception e) {
                Assert.fail("Error while writing to Pravega");
            } finally {
                latch.countDown();
            }
        };

        Runnable readTask = () -> {
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
                latch.countDown();
            }
        };

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(writeTask);
        executorService.execute(readTask);

        boolean wait = latch.await(30, TimeUnit.SECONDS);
        if (!wait) {
            Assert.fail("Read/Write operations taking more time to complete");
        }
        executorService.shutdown();
        if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
            executorService.shutdownNow();
        }
    }

    /**
     * Tests the {@link FlinkPravegaWriter} in {@code EXACTLY_ONCE} mode with watermarking.
     */
    @Test
    public void testExactlyOnceWriterWithWatermark() throws Exception {
        int numElements = 2000;

        // set up the stream
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        SETUP_UTILS.createTestStream(streamName, 4);

        CountDownLatch latch = new CountDownLatch(2);

        Runnable writeTask = () -> {
            // launch the Flink program that writes and has a failure during writing, to
            // make sure that this does not introduce any duplicates
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                    .setParallelism(1)
                    .enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L));
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            env.getConfig().setAutoWatermarkInterval(100);

            FlinkPravegaWriter<Integer> pravegaSink = FlinkPravegaWriter.<Integer>builder()
                    .forStream(streamName)
                    .withPravegaConfig(SETUP_UTILS.getPravegaConfig())
                    .withSerializationSchema(new IntSerializer())
                    .withEventRouter(event -> "fixedkey")
                    .withWriterMode(PravegaWriterMode.EXACTLY_ONCE)
                    .withTxnLeaseRenewalPeriod(Time.seconds(30))
                    .enableWatermark(true)
                    .build();

            env
                    .addSource(new ThrottledIntegerGeneratingSource(numElements).withWatermarks(20))
                    .map(new FailingMapper<>(numElements / 2))
                    .addSink(pravegaSink).setParallelism(2);

            try {
                env.execute();
            } catch (Exception e) {
                Assert.fail("Error while writing to Pravega");
            } finally {
                latch.countDown();
            }
        };

        Runnable readTask = () -> {
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

                latch.countDown();
            }
        };

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(writeTask);
        Thread.sleep(11000);
        executorService.execute(readTask);

        boolean wait = latch.await(1000, TimeUnit.SECONDS);
        if (!wait) {
            Assert.fail("Read/Write operations taking more time to complete");
        }
        executorService.shutdown();
        if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
            executorService.shutdownNow();
        }
    }

    // ----------------------------------------------------------------------------

    private static class IntSerializer implements SerializationSchema<Integer> {

        @Override
        public byte[] serialize(Integer integer) {
            return ByteBuffer.allocate(4).putInt(0, integer).array();
        }
    }
}
