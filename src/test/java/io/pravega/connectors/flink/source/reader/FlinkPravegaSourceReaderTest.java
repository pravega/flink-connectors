/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pravega.connectors.flink.source.reader;

import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.source.split.PravegaSplit;
import io.pravega.connectors.flink.utils.IntegerDeserializationSchema;
import io.pravega.connectors.flink.utils.SetupUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.core.io.InputStatus;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;

/** Unit tests for {@link PravegaSourceReader}. */
public class FlinkPravegaSourceReaderTest {

    private static final int NUM_SPLITS = 1;
    private static final int NUM_RECORDS_PER_SPLIT = 10;
    private static final int TOTAL_NUM_RECORDS = NUM_RECORDS_PER_SPLIT * NUM_SPLITS;

    private static final int NUM_PRAVEGA_SEGMENTS = 4;
    private static final String READER_GROUP_NAME = "flink-reader";
    private static final int READER0 = 0;
    private static final PravegaSplit SPLIT0 = new PravegaSplit(READER_GROUP_NAME, READER0);

    /** Setup utility */
    private static final SetupUtils SETUP_UTILS = new SetupUtils();

    @BeforeClass
    public static void setup() throws Exception {
        SETUP_UTILS.startAllServices();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        SETUP_UTILS.stopAllServices();
    }

    @Test
    public void testReadCheckpoint() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        SETUP_UTILS.createTestStream(streamName, NUM_PRAVEGA_SEGMENTS);
        createReaderGroup(streamName);
        try (PravegaSourceReader<Integer> reader = (PravegaSourceReader<Integer>) createReader(READER0, true)) {
            EventStreamWriter<Integer> eventWriter = SETUP_UTILS.getIntegerWriter(streamName);
            for (int i = 0; i < NUM_RECORDS_PER_SPLIT; i++) {
                eventWriter.writeEvent(i);
            }

            reader.addSplits(Collections.singletonList(SPLIT0));
            ReaderOutput<Integer> output = new TestingReaderOutput<>();

            InputStatus status = reader.pollNext(output);
            Assert.assertEquals(status, InputStatus.NOTHING_AVAILABLE);
        }
        deleteReaderGroup();
    }

    /** Simply test the reader reads all the splits fine. */
    @Test
    public void testRead() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        SETUP_UTILS.createTestStream(streamName, NUM_PRAVEGA_SEGMENTS);
        createReaderGroup(streamName);
        try (SourceReader<Integer, PravegaSplit> reader = createReader(READER0, false)) {
            EventStreamWriter<Integer> eventWriter = SETUP_UTILS.getIntegerWriter(streamName);
            for (int i = 0; i < NUM_RECORDS_PER_SPLIT; i++) {
                eventWriter.writeEvent(i);
            }

            reader.addSplits(getSplits());
            ValidatingSourceOutput output = new ValidatingSourceOutput();
            while (output.count() < TOTAL_NUM_RECORDS) {
                reader.pollNext(output);
            }
            output.validate();
        }
        deleteReaderGroup();
    }

    @Test
    public void testPollingFromEmptyQueue() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        SETUP_UTILS.createTestStream(streamName, NUM_PRAVEGA_SEGMENTS);
        createReaderGroup(streamName);
        EventStreamWriter<Integer> eventWriter = SETUP_UTILS.getIntegerWriter(streamName);
        for (int i = 0; i < NUM_RECORDS_PER_SPLIT; i++) {
            eventWriter.writeEvent(i);
        }

        ValidatingSourceOutput output = new ValidatingSourceOutput();
        List<PravegaSplit> splits =
                Collections.singletonList(getSplit(READER0));
        // Consumer all the records in the split.
        try (SourceReader<Integer, PravegaSplit> reader =
                     consumeRecords(splits, output, NUM_RECORDS_PER_SPLIT)) {
            // Now let the main thread poll again.
            assertEquals(
                    "The status should be ",
                    InputStatus.NOTHING_AVAILABLE,
                    reader.pollNext(output));
        }
        deleteReaderGroup();
    }

    @Test
    public void testAvailableOnEmptyQueue() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        SETUP_UTILS.createTestStream(streamName, NUM_PRAVEGA_SEGMENTS);
        createReaderGroup(streamName);
        EventStreamWriter<Integer> eventWriter = SETUP_UTILS.getIntegerWriter(streamName);
        for (int i = 0; i < NUM_RECORDS_PER_SPLIT; i++) {
            eventWriter.writeEvent(i);
        }

        // Consumer all the records in the split.
        try (SourceReader<Integer, PravegaSplit> reader = createReader(READER0, false)) {
            CompletableFuture<?> future = reader.isAvailable();
            Assert.assertFalse("There should be no records ready for poll.", future.isDone());
            // Add a split to the reader so there are more records to be read.
            reader.addSplits(
                    Collections.singletonList(
                            getSplit(READER0)));
            // The future should be completed fairly soon. Otherwise the test will hit timeout and
            // fail.
            future.get();
        }
        deleteReaderGroup();
    }

    private SourceReader<Integer, PravegaSplit> createReader(int subtaskId, boolean isCheckpointEvent) throws Exception {
        PravegaRecordEmitter emitter;
        if (isCheckpointEvent) {
            emitter = Mockito.mock(PravegaRecordEmitter.class);
            Mockito.when(emitter.getAndResetCheckpointId()).thenReturn(Optional.of(1L));
        } else {
            emitter = new PravegaRecordEmitter<>(new IntegerDeserializationSchema());
        }
        return new PravegaSourceReader<>(
                () -> new PravegaSplitReader(SETUP_UTILS.getScope(), SETUP_UTILS.getClientConfig(),
                        READER_GROUP_NAME, subtaskId),
                emitter,
                new Configuration(),
                new TestingReaderContext());
    }

    private static void createReaderGroup(String streamName) throws Exception {
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SETUP_UTILS.getScope(), SETUP_UTILS.getClientConfig());
        Stream stream = Stream.of(SETUP_UTILS.getScope(), streamName);
        readerGroupManager.createReaderGroup(READER_GROUP_NAME, ReaderGroupConfig.builder().stream(stream).disableAutomaticCheckpoints().build());
        readerGroupManager.close();
    }

    private static void deleteReaderGroup() throws Exception {
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SETUP_UTILS.getScope(), SETUP_UTILS.getClientConfig());
        readerGroupManager.getReaderGroup(READER_GROUP_NAME).close();
        readerGroupManager.deleteReaderGroup(READER_GROUP_NAME);
        readerGroupManager.close();
    }

    private List<PravegaSplit> getSplits() {
        List<PravegaSplit> splits = new ArrayList<>();
        splits.add(getSplit(READER0));
        return splits;
    }

    private PravegaSplit getSplit(int splitId) {
        return new PravegaSplit(READER_GROUP_NAME, splitId);
    }

    private SourceReader<Integer, PravegaSplit> consumeRecords(
            List<PravegaSplit> splits, ValidatingSourceOutput output, int n) throws Exception {
        SourceReader<Integer, PravegaSplit> reader = createReader(READER0, false);
        // Add splits to start the fetcher.
        reader.addSplits(splits);
        // Poll all the n records of the single split.
        while (output.count() < n) {
            reader.pollNext(output);
        }
        return reader;
    }

    public static class ValidatingSourceOutput implements ReaderOutput<Integer> {
        private Set<Integer> consumedValues = new HashSet<>();
        private int max = Integer.MIN_VALUE;
        private int min = Integer.MAX_VALUE;

        private int count = 0;

        @Override
        public void collect(Integer element) {
            max = Math.max(element, max);
            min = Math.min(element, min);
            count++;
            consumedValues.add(element);
        }

        @Override
        public void collect(Integer element, long timestamp) {
            collect(element);
        }

        public void validate() {

            assertEquals(
                    String.format("Should be %d distinct elements in total", TOTAL_NUM_RECORDS),
                    TOTAL_NUM_RECORDS,
                    consumedValues.size());
            assertEquals(
                    String.format("Should be %d elements in total", TOTAL_NUM_RECORDS),
                    TOTAL_NUM_RECORDS,
                    count);
            assertEquals("The min value should be 0", 0, min);
            assertEquals(
                    "The max value should be " + (TOTAL_NUM_RECORDS - 1),
                    TOTAL_NUM_RECORDS - 1,
                    max);
        }

        public int count() {
            return count;
        }

        @Override
        public void emitWatermark(Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public SourceOutput<Integer> createOutputForSplit(String splitId) {
            return this;
        }

        @Override
        public void releaseOutputForSplit(String splitId) {}
    }
}
