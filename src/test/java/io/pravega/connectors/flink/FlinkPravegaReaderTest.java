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

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventPointer;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.TimeWindow;
import io.pravega.client.stream.impl.EventPointerImpl;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.client.stream.impl.EventReadImpl;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.connectors.flink.serialization.JsonSerializer;
import io.pravega.connectors.flink.serialization.PravegaDeserializationSchema;
import io.pravega.connectors.flink.utils.IntegerDeserializationSchema;
import io.pravega.connectors.flink.utils.IntegerWithEventPointer;
import io.pravega.connectors.flink.utils.StreamSourceOperatorTestHarness;
import io.pravega.connectors.flink.watermark.AssignerWithTimeWindows;
import io.pravega.connectors.flink.watermark.LowerBoundAssigner;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.SerializedValue;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static io.pravega.connectors.flink.FlinkPravegaReader.ONLINE_READERS_METRICS_GAUGE;
import static io.pravega.connectors.flink.FlinkPravegaReader.PRAVEGA_READER_METRICS_GROUP;
import static io.pravega.connectors.flink.FlinkPravegaReader.READER_GROUP_METRICS_GROUP;
import static io.pravega.connectors.flink.FlinkPravegaReader.READER_GROUP_NAME_METRICS_GAUGE;
import static io.pravega.connectors.flink.FlinkPravegaReader.UNREAD_BYTES_METRICS_GAUGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
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
    private static final String SAMPLE_STREAM_NAME = "stream";
    private static final String SAMPLE_STREAM_NAME_2 = "stream-2";
    private static final String SAMPLE_COMPLETE_STREAM_NAME = SAMPLE_SCOPE + '/' + SAMPLE_STREAM_NAME;
    private static final Stream SAMPLE_STREAM = Stream.of(SAMPLE_SCOPE, SAMPLE_STREAM_NAME);
    private static final Stream SAMPLE_STREAM_2 = Stream.of(SAMPLE_SCOPE, SAMPLE_STREAM_NAME_2);
    private static final Segment SAMPLE_SEGMENT = new Segment(SAMPLE_SCOPE, SAMPLE_STREAM.getStreamName(), 1);
    private static final StreamCut SAMPLE_CUT = new StreamCutImpl(SAMPLE_STREAM, Collections.singletonMap(SAMPLE_SEGMENT, 42L));
    private static final StreamCut SAMPLE_CUT2 = new StreamCutImpl(SAMPLE_STREAM, Collections.singletonMap(SAMPLE_SEGMENT, 1024L));

    private static final Time GROUP_REFRESH_TIME = Time.seconds(10);
    private static final String GROUP_NAME = "group";

    private static final IntegerDeserializationSchema DESERIALIZATION_SCHEMA = new TestDeserializationSchema();
    private static final Time READER_TIMEOUT = Time.seconds(1);
    private static final Time CHKPT_TIMEOUT = Time.seconds(1);
    private static final int MAX_OUTSTANDING_CHECKPOINT_REQUEST = 5;

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

        try (StreamSourceOperatorTestHarness<Integer, TestableFlinkPravegaReader<Integer>> testHarness =
                     createTestHarness(reader, 1, 1, 0, TimeCharacteristic.ProcessingTime)) {
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

            // verify if metrics are generated
            MetricGroup pravegaReaderMetricGroup = testHarness.getMetricGroup().addGroup(PRAVEGA_READER_METRICS_GROUP);
            MetricGroup readerGroupMetricGroup = pravegaReaderMetricGroup.addGroup(READER_GROUP_METRICS_GROUP);
            String scopeString = ScopeFormat.concat('.', readerGroupMetricGroup.getScopeComponents());

            validateMetricGroup(scopeString, UNREAD_BYTES_METRICS_GAUGE, readerGroupMetricGroup);
            validateMetricGroup(scopeString, READER_GROUP_NAME_METRICS_GAUGE, readerGroupMetricGroup);
            validateMetricGroup(scopeString, UNREAD_BYTES_METRICS_GAUGE, readerGroupMetricGroup);
            validateMetricGroup(scopeString, ONLINE_READERS_METRICS_GAUGE, readerGroupMetricGroup);
            validateMetricGroup(scopeString, UNREAD_BYTES_METRICS_GAUGE, readerGroupMetricGroup);

        }
    }

    /**
     * Tests the behavior of {@code run()} with TruncatedDataException.
     */
    @Test
    public void testTruncated() throws Exception {
        TestableFlinkPravegaReader<Integer> reader = createReader();

        try (StreamSourceOperatorTestHarness<Integer, TestableFlinkPravegaReader<Integer>> testHarness =
                     createTestHarness(reader, 1, 1, 0, TimeCharacteristic.ProcessingTime)) {
            testHarness.open();

            // prepare a sequence of events
            TestEventGenerator<Integer> evts = new TestEventGenerator<>();
            when(reader.eventStreamReader.readNextEvent(anyLong()))
                    .thenReturn(evts.event(1))
                    .thenThrow(new TruncatedDataException())
                    .thenReturn(evts.event(2))
                    .thenReturn(evts.event(TestDeserializationSchema.END_OF_STREAM));

            // run the source
            testHarness.run();

            // verify that the event stream was read until the end of stream
            verify(reader.eventStreamReader, times(4)).readNextEvent(anyLong());
            Queue<Object> actual = testHarness.getOutput();
            Queue<Object> expected = new ConcurrentLinkedQueue<>();
            expected.add(record(1));
            expected.add(record(2));
            TestHarnessUtil.assertOutputEquals("Unexpected output", expected, actual);
        }
    }

    /**
     * Tests the behavior of {@code run()} when deserialized with metadata.
     */
    @Test
    public void testRunWithMetadata() throws Exception {
        TestableFlinkPravegaReader<IntegerWithEventPointer> reader = createReaderWithMetadata(true);

        try (StreamSourceOperatorTestHarness<IntegerWithEventPointer, TestableFlinkPravegaReader<IntegerWithEventPointer>> testHarness =
                     createTestHarness(reader, 1, 1, 0, TimeCharacteristic.ProcessingTime)) {
            testHarness.open();

            // prepare a sequence of events
            TestEventGenerator<IntegerWithEventPointer> evts = new TestEventGenerator<>();
            when(reader.eventStreamReader.readNextEvent(anyLong()))
                    .thenReturn(evts.event(new IntegerWithEventPointer(1), 1))
                    .thenReturn(evts.event(new IntegerWithEventPointer(IntegerWithEventPointer.END_OF_STREAM), 2));

            // run the source
            testHarness.run();

            // verify that the event stream was read until the end of stream
            verify(reader.eventStreamReader, times(2)).readNextEvent(anyLong());
            Queue<Object> actual = testHarness.getOutput();
            assertEquals(actual.size(), 1);

            // verify that the event contains the right value and EventPointer information
            @SuppressWarnings("unchecked")
            IntegerWithEventPointer output = ((StreamRecord<IntegerWithEventPointer>) actual.peek()).getValue();
            assertEquals(output.getValue(), 1);

            EventPointer outputEventPointer = EventPointer.fromBytes(ByteBuffer.wrap(
                    output.getEventPointerBytes()));
            assertEquals(outputEventPointer, evts.getEventPointer(1));
        }

        // Test default implementation of extractEvent
        reader = createReaderWithMetadata(false);
        try (StreamSourceOperatorTestHarness<IntegerWithEventPointer, TestableFlinkPravegaReader<IntegerWithEventPointer>> testHarness =
                     createTestHarness(reader, 1, 1, 0, TimeCharacteristic.ProcessingTime)) {
            testHarness.open();

            // prepare a sequence of events
            TestEventGenerator<IntegerWithEventPointer> evts = new TestEventGenerator<>();
            when(reader.eventStreamReader.readNextEvent(anyLong()))
                    .thenReturn(evts.event(new IntegerWithEventPointer(1), 1))
                    .thenReturn(evts.event(new IntegerWithEventPointer(IntegerWithEventPointer.END_OF_STREAM), 2));

            // run the source
            testHarness.run();

            // verify that the event stream was read until the end of stream
            verify(reader.eventStreamReader, times(2)).readNextEvent(anyLong());
            Queue<Object> actual = testHarness.getOutput();
            assertEquals(actual.size(), 1);

            // verify that the event contains the right value and no EventPointer information
            @SuppressWarnings("unchecked")
            IntegerWithEventPointer output = ((StreamRecord<IntegerWithEventPointer>) actual.peek()).getValue();
            assertEquals(output.getValue(), 1);
            assertNull(output.getEventPointerBytes());
        }
    }


    /**
     * Tests the behavior of {@code run()} with watermark.
     */
    @Test
    public void testRunWithWatermark() throws Exception {
        TestableFlinkPravegaReader<Integer> reader = createReaderWithWatermark(new LowerBoundAssigner<Integer>() {
            @Override
            public long extractTimestamp(Integer element, long previousElementTimestamp) {
                return element;
            }
        });

        try (StreamSourceOperatorTestHarness<Integer, TestableFlinkPravegaReader<Integer>> testHarness =
                     createTestHarness(reader, 1, 1, 0, TimeCharacteristic.EventTime)) {
            // reset the auto watermark interval to 50 millisecond
            testHarness.getExecutionConfig().setAutoWatermarkInterval(50);
            testHarness.open();

            // prepare a sequence of events with processing time progress
            TestEventGenerator<Integer> evts = new TestEventGenerator<>();
            when(reader.eventStreamReader.readNextEvent(anyLong()))
                    .thenAnswer((Answer<EventRead<Integer>>) invocation -> {
                        testHarness.setProcessingTime(1);
                        return evts.event(1);
                    })
                    .thenAnswer((Answer<EventRead<Integer>>) invocation -> {
                        testHarness.setProcessingTime(51);
                        return evts.event(2);
                    })
                    .thenAnswer((Answer<EventRead<Integer>>) invocation -> {
                        testHarness.setProcessingTime(101);
                        return evts.event(TestDeserializationSchema.END_OF_STREAM);
                    });
            when(reader.eventStreamReader.getCurrentTimeWindow(anyObject()))
                    .thenReturn(new TimeWindow(1L, 2L))
                    .thenReturn(new TimeWindow(2L, 3L));

            // run the source
            testHarness.run();

            // verify that the event stream was read until the end of stream
            verify(reader.eventStreamReader, times(3)).readNextEvent(anyLong());
            verify(reader.eventStreamReader, times(2)).getCurrentTimeWindow(anyObject());

            Queue<Object> actual = testHarness.getOutput();
            Queue<Object> expected = new ConcurrentLinkedQueue<>();
            expected.add(record(1, 1));
            expected.add(watermark(1));
            expected.add(record(2, 2));
            expected.add(watermark(2));

            TestHarnessUtil.assertOutputEquals("Unexpected output", expected, actual);
        }
    }


    /**
     * helper method to validate the metrics
     */
    private void validateMetricGroup(String prefix, String metric, MetricGroup readerGroupMetricGroup) {
        String expectedValue = prefix.concat(".").concat(metric);
        Assert.assertTrue(metric, expectedValue.equals(readerGroupMetricGroup.getMetricIdentifier(metric)));
    }

    /**
     * Tests the cancellation support.
     */
    @Test
    public void testCancellation() throws Exception {
        TestableFlinkPravegaReader<Integer> reader = createReader();

        try (StreamSourceOperatorTestHarness<Integer, TestableFlinkPravegaReader<Integer>> testHarness =
                     createTestHarness(reader, 1, 1, 0, TimeCharacteristic.ProcessingTime)) {
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
        boolean enableMetrics = true;
        return new TestableFlinkPravegaReader<>(
                "hookUid", clientConfig, rgConfig, SAMPLE_SCOPE, GROUP_NAME, DESERIALIZATION_SCHEMA,
                null, READER_TIMEOUT, CHKPT_TIMEOUT, enableMetrics);
    }

    /**
     * Creates a {@link TestableFlinkPravegaReader} with event time and watermarking.
     */
    private static TestableFlinkPravegaReader<Integer> createReaderWithWatermark(AssignerWithTimeWindows<Integer> assignerWithTimeWindows) {
        ClientConfig clientConfig = ClientConfig.builder().build();
        ReaderGroupConfig rgConfig = ReaderGroupConfig.builder().stream(SAMPLE_STREAM).build();
        boolean enableMetrics = true;

        try {
            ClosureCleaner.clean(assignerWithTimeWindows, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
            SerializedValue<AssignerWithTimeWindows<Integer>> serializedAssigner =
                    new SerializedValue<>(assignerWithTimeWindows);
            return new TestableFlinkPravegaReader<>(
                    "hookUid", clientConfig, rgConfig, SAMPLE_SCOPE, GROUP_NAME, DESERIALIZATION_SCHEMA,
                    serializedAssigner, READER_TIMEOUT, CHKPT_TIMEOUT, enableMetrics);
        } catch (IOException e) {
            throw new IllegalArgumentException("The given assigner is not serializable", e);
        }
    }

    /**
     * Creates a {@link TestableFlinkPravegaReader} with metadata deserialization.
     */
    private static TestableFlinkPravegaReader<IntegerWithEventPointer> createReaderWithMetadata(boolean includeMetadata) {
        ClientConfig clientConfig = ClientConfig.builder().build();
        ReaderGroupConfig rgConfig = ReaderGroupConfig.builder().stream(SAMPLE_STREAM).build();
        boolean enableMetrics = true;

        return new TestableFlinkPravegaReader<>(
                    "hookUid", clientConfig, rgConfig, SAMPLE_SCOPE, GROUP_NAME,
                    new TestMetadataDeserializationSchema(includeMetadata), null, READER_TIMEOUT, CHKPT_TIMEOUT, enableMetrics);
    }

    /**
     * Creates a test harness for a {@link SourceFunction}.
     */
    private <T, F extends SourceFunction<T>> StreamSourceOperatorTestHarness<T, F> createTestHarness(
            F sourceFunction, int maxParallelism, int parallelism, int subtaskIndex, TimeCharacteristic timeCharacteristic) throws Exception {
        StreamSourceOperatorTestHarness harness = new StreamSourceOperatorTestHarness<T, F>(sourceFunction, maxParallelism, parallelism, subtaskIndex);
        harness.setTimeCharacteristic(timeCharacteristic);
        return harness;
    }

    /**
     * Creates a {@link StreamRecord} for the given event (without a timestamp).
     */
    private static <T> StreamRecord<T> record(T evt) {
        return new StreamRecord<>(evt);
    }

    /**
     * Creates a {@link StreamRecord} for the given event with a timestamp.
     */
    private static <T> StreamRecord<T> record(T evt, long timestamp) {
        return new StreamRecord<>(evt, timestamp);
    }

    /**
     * Creates a {@link Watermark} with a timestamp.
     */
    private static Watermark watermark(long timestamp) {
        return new Watermark(timestamp);
    }


    // endregion

    // region Builder Tests

    @Test
    public void testBuilderProperties() {
        TestableStreamingReaderBuilder builder = new TestableStreamingReaderBuilder()
                .forStream(SAMPLE_STREAM, SAMPLE_CUT)
                .withReaderGroupScope(SAMPLE_SCOPE)
                .withReaderGroupName(GROUP_NAME)
                .withMaxOutstandingCheckpointRequest(MAX_OUTSTANDING_CHECKPOINT_REQUEST)
                .withReaderGroupRefreshTime(GROUP_REFRESH_TIME);

        FlinkPravegaReader<Integer> reader = builder.buildSourceFunction();

        assertNotNull(reader.hookUid);
        assertNotNull(reader.clientConfig);
        assertEquals(-1L, reader.readerGroupConfig.getAutomaticCheckpointIntervalMillis());
        assertEquals(MAX_OUTSTANDING_CHECKPOINT_REQUEST, reader.readerGroupConfig.getMaxOutstandingCheckpointRequest());
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
                .forStream(SAMPLE_STREAM, SAMPLE_CUT, SAMPLE_CUT2)
                .withEventReadTimeout(Time.seconds(42L));
        String uid2 = builder2.generateUid();

        TestableStreamingReaderBuilder builder3 = new TestableStreamingReaderBuilder()
                .withReaderGroupScope(SAMPLE_SCOPE)
                .forStream(SAMPLE_STREAM_2);
        String uid3 = builder3.generateUid();

        assertEquals(uid1, uid2);
        assertNotEquals(uid1, uid3);
    }

    // endregion

    // region Helper Classes

    /**
     * Generates a sequence of {@link EventRead} instances, including events, checkpoints, and idleness.
     */
    private static class TestEventGenerator<T> {

        private String buildEventPointerString(long offset) {
            StringBuilder sb = new StringBuilder();
            sb.append(SAMPLE_SEGMENT.getScopedName());
            sb.append(':');
            sb.append(offset);
            sb.append('-');
            sb.append(1);
            return sb.toString();
        }

        public EventPointer getEventPointer(long offset) {
            return EventPointerImpl.fromString(buildEventPointerString(offset));
        }

        public EventRead<T> event(T evt) {
            return new EventReadImpl<>(evt, mock(Position.class), mock(EventPointer.class), null);
        }

        public EventRead<T> event(T evt, long offset) {
            return new EventReadImpl<>(evt, mock(Position.class), getEventPointer(offset), null);
        }

        public EventRead<T> idle() {
            return event(null);
        }

        @SuppressWarnings("unchecked")
        public EventRead<T> checkpoint(long checkpointId) {
            String checkpointName = ReaderCheckpointHook.createCheckpointName(checkpointId);
            return new EventReadImpl<>(null, mock(Position.class), mock(EventPointer.class), checkpointName);
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
     * A test JSON format deserialization schema with metadata.
     */
    private static class TestMetadataDeserializationSchema extends PravegaDeserializationSchema<IntegerWithEventPointer> {
        private boolean includeMetadata;

        public TestMetadataDeserializationSchema(boolean includeMetadata) {
            super(IntegerWithEventPointer.class, new JsonSerializer<>(IntegerWithEventPointer.class));
            this.includeMetadata = includeMetadata;
        }

        @Override
        public IntegerWithEventPointer extractEvent(EventRead<IntegerWithEventPointer> eventRead) {
            if (!includeMetadata) {
                return super.extractEvent(eventRead);
            }

            IntegerWithEventPointer event = eventRead.getEvent();
            event.setEventPointer(eventRead.getEventPointer());
            return event;
        }

        @Override
        public boolean isEndOfStream(IntegerWithEventPointer nextElement) {
            return nextElement.isEndOfStream();
        }
    }

    /**
     * A reader builder subclass for test purposes.
     */
    private static class TestableFlinkPravegaReader<T> extends FlinkPravegaReader<T> {

        @SuppressWarnings("unchecked")
        final ReaderGroupManager readerGroupManager = mock(ReaderGroupManager.class);

        @SuppressWarnings("unchecked")
        final ReaderGroup readerGroup = mock(ReaderGroup.class);

        @SuppressWarnings("unchecked")
        final EventStreamReader<T> eventStreamReader = mock(EventStreamReader.class);

        protected TestableFlinkPravegaReader(String hookUid, ClientConfig clientConfig,
                                             ReaderGroupConfig readerGroupConfig, String readerGroupScope,
                                             String readerGroupName, DeserializationSchema<T> deserializationSchema,
                                             SerializedValue<AssignerWithTimeWindows<T>> assignerWithTimeWindows,
                                             Time eventReadTimeout, Time checkpointInitiateTimeout,
                                             boolean enableMetrics) {
            super(hookUid, clientConfig, readerGroupConfig, readerGroupScope, readerGroupName, deserializationSchema,
                    assignerWithTimeWindows, eventReadTimeout, checkpointInitiateTimeout, enableMetrics);
        }

        @Override
        protected ReaderGroupManager createReaderGroupManager() {
            doNothing().when(readerGroupManager).createReaderGroup(readerGroupName, readerGroupConfig);
            doReturn(new HashSet<>(Arrays.asList(SAMPLE_COMPLETE_STREAM_NAME))).when(readerGroup).getStreamNames();
            doReturn(readerGroup).when(readerGroupManager).getReaderGroup(readerGroupName);
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

        @Override
        protected SerializedValue<AssignerWithTimeWindows<Integer>> getAssignerWithTimeWindows() {
            return null;
        }
    }

    // endregion
}