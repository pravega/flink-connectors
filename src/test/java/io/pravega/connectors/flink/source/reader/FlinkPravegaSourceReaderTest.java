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
import io.pravega.connectors.flink.util.FlinkPravegaUtils;
import io.pravega.connectors.flink.utils.IntegerDeserializationSchema;
import io.pravega.connectors.flink.utils.PravegaTestEnvironment;
import io.pravega.connectors.flink.utils.runtime.PravegaRuntime;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.testutils.source.reader.SourceReaderTestBase;
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
import java.util.List;
import java.util.Optional;

/** Unit tests for {@link PravegaSourceReader}. */
public class FlinkPravegaSourceReaderTest extends SourceReaderTestBase<PravegaSplit> {

    private static final int NUM_SPLITS = 1;
    private static final int NUM_PRAVEGA_SEGMENTS = 4;
    private static final int READER0 = 0;

    private static final PravegaTestEnvironment PRAVEGA = new PravegaTestEnvironment(PravegaRuntime.CONTAINER);

    private String readerGroupName;

    @BeforeClass
    public static void setupPravega() throws Exception {
        PRAVEGA.startUp();
    }

    @AfterClass
    public static void tearDownPravega() throws Exception {
        PRAVEGA.tearDown();
    }

    protected int getNumSplits() {
        return NUM_SPLITS;
    }

    // ------------------------------------------

    @Test
    public void testReadCheckpoint() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        final String readerGroupName = FlinkPravegaUtils.generateRandomReaderGroupName();
        final PravegaSplit split = new PravegaSplit(readerGroupName, READER0);
        PRAVEGA.operator().createTestStream(streamName, NUM_PRAVEGA_SEGMENTS);
        createReaderGroup(readerGroupName, streamName);
        try (final SourceReader<Integer, PravegaSplit> reader = createReader(readerGroupName, READER0, true);
             final EventStreamWriter<Integer> eventWriter = PRAVEGA.operator().getIntegerWriter(streamName)) {
            for (int i = 0; i < NUM_RECORDS_PER_SPLIT; i++) {
                eventWriter.writeEvent(i);
            }

            reader.addSplits(Collections.singletonList(split));
            ReaderOutput<Integer> output = new TestingReaderOutput<>();

            InputStatus status = reader.pollNext(output);
            Assert.assertEquals(status, InputStatus.NOTHING_AVAILABLE);
        }
    }

    // Pravega doesn't support partial failover recover, so we override this method in test base class
    @Test
    public void testSnapshot() throws Exception {

    }

    // ------------------------------------------

    @Override
    protected SourceReader<Integer, PravegaSplit> createReader() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        final String readerGroupName = FlinkPravegaUtils.generateRandomReaderGroupName();
        setReaderGroupName(readerGroupName);
        PRAVEGA.operator().createTestStream(streamName, NUM_PRAVEGA_SEGMENTS);
        createReaderGroup(readerGroupName, streamName);

        try (final EventStreamWriter<Integer> eventWriter = PRAVEGA.operator().getIntegerWriter(streamName)) {
            for (int i = 0; i < NUM_RECORDS_PER_SPLIT; i++) {
                eventWriter.writeEvent(i);
            }
        }

        return createReader(readerGroupName, READER0, false);
    }

    private SourceReader<Integer, PravegaSplit> createReader(
            String readerGroupName, int subtaskId, boolean isCheckpointEvent) {
        PravegaRecordEmitter emitter;
        if (isCheckpointEvent) {
            emitter = Mockito.mock(PravegaRecordEmitter.class);
            Mockito.when(emitter.getAndResetCheckpointId()).thenReturn(Optional.of(1L));
        } else {
            emitter = new PravegaRecordEmitter<>(new IntegerDeserializationSchema());
        }
        return new PravegaSourceReader<>(
                () -> new PravegaSplitReader(PRAVEGA.operator().getScope(), PRAVEGA.operator().getClientConfig(),
                        readerGroupName, subtaskId),
                emitter,
                new Configuration(),
                new TestingReaderContext());
    }

    @Override
    protected List<PravegaSplit> getSplits(int numSplits, int numRecordsPerSplit, Boundedness boundedness) {
        return getSplits(readerGroupName);
    }

    private List<PravegaSplit> getSplits(String readerGroupName) {
        List<PravegaSplit> splits = new ArrayList<>();
        splits.add(getSplit(readerGroupName, READER0));
        return splits;
    }

    @Override
    protected PravegaSplit getSplit(int splitId, int numRecords, Boundedness boundedness) {
        return getSplit(readerGroupName, splitId);
    }

    private PravegaSplit getSplit(String readerGroupName, int splitId) {
        return new PravegaSplit(readerGroupName, splitId);
    }

    @Override
    protected long getNextRecordIndex(PravegaSplit split) {
        return NUM_RECORDS_PER_SPLIT;
    }

    private static void createReaderGroup(String readerGroupName, String streamName) {
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(PRAVEGA.operator().getScope(), PRAVEGA.operator().getClientConfig());
        Stream stream = Stream.of(PRAVEGA.operator().getScope(), streamName);
        readerGroupManager.createReaderGroup(readerGroupName, ReaderGroupConfig.builder().stream(stream).disableAutomaticCheckpoints().build());
        readerGroupManager.close();
    }

    private void setReaderGroupName(String readerGroupName) {
        this.readerGroupName = readerGroupName;
    }
}
