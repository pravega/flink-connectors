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

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventPointer;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Sequence;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.EventReadImpl;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.connectors.flink.utils.IntegerDeserializationSchema;
import io.pravega.connectors.flink.utils.StreamSourceOperatorTestHarness;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.junit.Test;

import java.util.Collections;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * Unit tests for {@link FlinkPravegaReader} and its builder.
 */
public class FlinkPravegaReaderTest {

    private static final String SAMPLE_SCOPE = "scope";
    private static final Stream SAMPLE_STREAM = Stream.of(SAMPLE_SCOPE, "stream");
    private static final Segment SAMPLE_SEGMENT = new Segment(SAMPLE_SCOPE, SAMPLE_STREAM.getStreamName(), 1);
    private static final StreamCut SAMPLE_CUT = new StreamCutImpl(SAMPLE_STREAM, Collections.singletonMap(SAMPLE_SEGMENT, 42L));
    private static final StreamCut SAMPLE_CUT2 = new StreamCutImpl(SAMPLE_STREAM, Collections.singletonMap(SAMPLE_SEGMENT, 1024L));

    private static final Time GROUP_REFRESH_TIME = Time.seconds(10);
    private static final String GROUP_NAME = "group";

    private static final IntegerDeserializationSchema DESERIALIZATION_SCHEMA = new TestDeserializationSchema();
    private static final Time READER_TIMEOUT = Time.seconds(1);
    private static final Time CHKPT_TIMEOUT = Time.seconds(1);

    // region Source Function Tests

    /**
     * Tests the behavior of {@code initialize()}.
     */
    @Test
    public void testInitialize() {
        TestableFlinkPravegaReader<Integer> reader = createReader();
        reader.initialize();
        verify(reader.readerGroupManager).createReaderGroup(GROUP_NAME, reader.readerGroupConfig);
    }

    /**
     * Tests the behavior of {@code run()}.
     */
    @Test
    public void testRun() throws Exception {
        TestableFlinkPravegaReader<Integer> reader = createReader();

        try (StreamSourceOperatorTestHarness<Integer, TestableFlinkPravegaReader<Integer>> testHarness = createTestHarness(reader, 1, 1, 0)) {
            testHarness.open();

            // prepare a sequence of events
            TestEventGenerator<Integer> evts = new TestEventGenerator<>();
            when(reader.eventStreamReader.readNextEvent(anyLong()))
                    .thenReturn(evts.event(1))
                    .thenReturn(evts.event(2))
                    .thenReturn(evts.checkpoint(42L))
                    .thenReturn(evts.idle())
                    .thenReturn(evts.event(3))
                    .thenReturn(evts.event(TestDeserializationSchema.END_OF_STREAM));

            // run the source
            testHarness.run();

            // verify that the event stream was read until the end of stream
            verify(reader.eventStreamReader, times(6)).readNextEvent(anyLong());
            Queue<Object> actual = testHarness.getOutput();
            Queue<Object> expected = new ConcurrentLinkedQueue<>();
            expected.add(record(1));
            expected.add(record(2));
            expected.add(record(3));
            TestHarnessUtil.assertOutputEquals("Unexpected output", expected, actual);

            // verify that checkpoints were triggered
            Queue<Long> actualChkpts = testHarness.getTriggeredCheckpoints();
            Queue<Long> expectedChkpts = new ConcurrentLinkedQueue<>();
            expectedChkpts.add(42L);
            TestHarnessUtil.assertOutputEquals("Unexpected checkpoints", expectedChkpts, actualChkpts);
        }
    }

    /**
     * Tests the cancellation support.
     */
    @Test
    public void testCancellation() throws Exception {
        TestableFlinkPravegaReader<Integer> reader = createReader();

        try (StreamSourceOperatorTestHarness<Integer, TestableFlinkPravegaReader<Integer>> testHarness = createTestHarness(reader, 1, 1, 0)) {
            testHarness.open();

            // prepare a sequence of events
            TestEventGenerator<Integer> evts = new TestEventGenerator<>();
            when(reader.eventStreamReader.readNextEvent(anyLong()))
                    .thenAnswer(i -> {
                        testHarness.cancel();
                        return evts.idle();
                    });

            // run the source, which should return upon cancellation
            testHarness.run();
            assertFalse(reader.running);
        }
    }

    /**
     * Creates a {@link TestableFlinkPravegaReader}.
     */
    private static TestableFlinkPravegaReader<Integer> createReader() {
        ClientConfig clientConfig = ClientConfig.builder().build();
        ReaderGroupConfig rgConfig = ReaderGroupConfig.builder().stream(SAMPLE_STREAM).build();
        return new TestableFlinkPravegaReader<>(
                "hookUid", clientConfig, rgConfig, SAMPLE_SCOPE, GROUP_NAME, DESERIALIZATION_SCHEMA, READER_TIMEOUT, CHKPT_TIMEOUT);
    }

    /**
     * Creates a test harness for a {@link SourceFunction}.
     */
    private <T, F extends SourceFunction<T>> StreamSourceOperatorTestHarness<T, F> createTestHarness(
            F sourceFunction, int maxParallelism, int parallelism, int subtaskIndex) throws Exception {
        StreamSourceOperatorTestHarness harness = new StreamSourceOperatorTestHarness<T, F>(sourceFunction, maxParallelism, parallelism, subtaskIndex);
        harness.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        return harness;
    }

    /**
     * Creates a {@link StreamRecord} for the given event (without a timestamp).
     */
    private static <T> StreamRecord<T> record(T evt) {
        return new StreamRecord<>(evt);
    }

    // endregion

    // region Builder Tests

    @Test
    public void testBuilderProperties() {
        TestableStreamingReaderBuilder builder = new TestableStreamingReaderBuilder()
                .forStream(SAMPLE_STREAM, SAMPLE_CUT)
                .withReaderGroupScope(SAMPLE_SCOPE)
                .withReaderGroupName(GROUP_NAME)
                .withReaderGroupRefreshTime(GROUP_REFRESH_TIME);

        FlinkPravegaReader<Integer> reader = builder.buildSourceFunction();

        assertNotNull(reader.hookUid);
        assertNotNull(reader.clientConfig);
        assertEquals(-1L, reader.readerGroupConfig.getAutomaticCheckpointIntervalMillis());
        assertEquals(GROUP_REFRESH_TIME.toMilliseconds(), reader.readerGroupConfig.getGroupRefreshTimeMillis());
        assertEquals(GROUP_NAME, reader.readerGroupName);
        assertEquals(Collections.singletonMap(SAMPLE_STREAM, SAMPLE_CUT), reader.readerGroupConfig.getStartingStreamCuts());
        assertEquals(DESERIALIZATION_SCHEMA, reader.deserializationSchema);
        assertEquals(DESERIALIZATION_SCHEMA.getProducedType(), reader.getProducedType());
        assertNotNull(reader.eventReadTimeout);
        assertNotNull(reader.checkpointInitiateTimeout);
    }

    @Test
    public void testRgScope() {
        PravegaConfig config = new PravegaConfig(new Properties(), Collections.emptyMap(), ParameterTool.fromMap(Collections.emptyMap()));

        // no scope
        TestableStreamingReaderBuilder builder = new TestableStreamingReaderBuilder()
                .forStream(SAMPLE_STREAM, SAMPLE_CUT)
                .withPravegaConfig(config);

        FlinkPravegaReader<Integer> reader;
        try {
            builder.buildSourceFunction();
            fail();
        } catch (IllegalStateException e) {
            // "missing reader group scope"
        }

        // default scope
        config.withDefaultScope(SAMPLE_SCOPE);
        reader = builder.buildSourceFunction();
        assertEquals(SAMPLE_SCOPE, reader.readerGroupScope);

        // explicit scope
        builder.withReaderGroupScope("myscope");
        reader = builder.buildSourceFunction();
        assertEquals("myscope", reader.readerGroupScope);
    }

    @Test
    public void testGenerateUid() {
        TestableStreamingReaderBuilder builder1 = new TestableStreamingReaderBuilder()
                .withReaderGroupScope(SAMPLE_SCOPE)
                .forStream(SAMPLE_STREAM, SAMPLE_CUT, StreamCut.UNBOUNDED);
        String uid1 = builder1.generateUid();

        TestableStreamingReaderBuilder builder2 = new TestableStreamingReaderBuilder()
                .withReaderGroupScope(SAMPLE_SCOPE)
                .forStream(SAMPLE_STREAM, SAMPLE_CUT, StreamCut.UNBOUNDED)
                .withEventReadTimeout(Time.seconds(42L));
        String uid2 = builder2.generateUid();

        TestableStreamingReaderBuilder builder3 = new TestableStreamingReaderBuilder()
                .withReaderGroupScope(SAMPLE_SCOPE)
                .forStream(SAMPLE_STREAM, SAMPLE_CUT2, StreamCut.UNBOUNDED);
        String uid3 = builder3.generateUid();

        TestableStreamingReaderBuilder builder4 = new TestableStreamingReaderBuilder()
                .withReaderGroupScope(SAMPLE_SCOPE)
                .forStream(SAMPLE_STREAM, SAMPLE_CUT, SAMPLE_CUT2);
        String uid4 = builder4.generateUid();

        assertEquals(uid1, uid2);
        assertNotEquals(uid1, uid3);
        assertNotEquals(uid1, uid4);
    }

    // endregion

    // region Helper Classes

    /**
     * Generates a sequence of {@link EventRead} instances, including events, checkpoints, and idleness.
     */
    private static class TestEventGenerator<T> {
        private long sequence = 0;

        public EventRead<T> event(T evt) {
            return new EventReadImpl<>(Sequence.create(0, sequence++), evt, mock(Position.class), mock(EventPointer.class), null);
        }

        public EventRead<T> idle() {
            return event(null);
        }

        @SuppressWarnings("unchecked")
        public EventRead<T> checkpoint(long checkpointId) {
            String checkpointName = ReaderCheckpointHook.createCheckpointName(checkpointId);
            return new EventReadImpl<>(Sequence.create(0, sequence++), null, mock(Position.class), mock(EventPointer.class), checkpointName);
        }
    }

    /**
     * A deserialization schema for test purposes.
     */
    private static class TestDeserializationSchema extends IntegerDeserializationSchema {
        public static final int END_OF_STREAM = -1;
        @Override
        public boolean isEndOfStream(Integer nextElement) {
            return nextElement.equals(END_OF_STREAM);
        }
    }

    /**
     * A reader builder subclass for test purposes.
     */
    private static class TestableFlinkPravegaReader<T> extends FlinkPravegaReader<T> {

        @SuppressWarnings("unchecked")
        final ReaderGroupManager readerGroupManager = mock(ReaderGroupManager.class);

        @SuppressWarnings("unchecked")
        final EventStreamReader<T> eventStreamReader = mock(EventStreamReader.class);

        protected TestableFlinkPravegaReader(String hookUid, ClientConfig clientConfig, ReaderGroupConfig readerGroupConfig, String readerGroupScope, String readerGroupName, DeserializationSchema<T> deserializationSchema, Time eventReadTimeout, Time checkpointInitiateTimeout) {
            super(hookUid, clientConfig, readerGroupConfig, readerGroupScope, readerGroupName, deserializationSchema, eventReadTimeout, checkpointInitiateTimeout);
        }

        @Override
        protected ReaderGroupManager createReaderGroupManager() {
            doNothing().when(readerGroupManager).createReaderGroup(readerGroupName, readerGroupConfig);
            doReturn(mock(ReaderGroup.class)).when(readerGroupManager).getReaderGroup(readerGroupName);
            return readerGroupManager;
        }

        @Override
        protected EventStreamReader<T> createEventStreamReader(String readerId) {
            return eventStreamReader;
        }
    }

    /**
     * A reader subclass for test purposes.
     */
    private static class TestableStreamingReaderBuilder extends AbstractStreamingReaderBuilder<Integer, TestableStreamingReaderBuilder> {
        @Override
        protected TestableStreamingReaderBuilder builder() {
            return this;
        }

        @Override
        protected DeserializationSchema<Integer> getDeserializationSchema() {
            return DESERIALIZATION_SCHEMA;
        }
    }

    // endregion
}