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
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.source.split.PravegaSplit;
import io.pravega.connectors.flink.util.FlinkPravegaUtils;
import io.pravega.connectors.flink.utils.SetupUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.mock.Whitebox;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Unit tests for {@link PravegaSplitReader}. */
public class FlinkPravegaSplitReaderTest {
    private static final int NUM_PRAVEGA_SEGMENTS = 4;
    private static final int READER0 = 0;
    private static final int NUM_EVENTS = 100;

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
    public void testHandleSplitChangesAndFetch() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        final String readerGroupName = FlinkPravegaUtils.generateRandomReaderGroupName();
        final PravegaSplit split = new PravegaSplit(readerGroupName, READER0);
        SETUP_UTILS.createTestStream(streamName, NUM_PRAVEGA_SEGMENTS);
        createReaderGroup(readerGroupName, streamName);
        PravegaSplitReader reader = createSplitReader(READER0, readerGroupName);
        assignSplitsAndFetchUntilFinish(reader, split, streamName);
        reader.close();
    }

    @Test
    public void testWakeUp() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        final String readerGroupName = FlinkPravegaUtils.generateRandomReaderGroupName();
        final PravegaSplit split = new PravegaSplit(readerGroupName, READER0);
        SETUP_UTILS.createTestStream(streamName, NUM_PRAVEGA_SEGMENTS);
        createReaderGroup(readerGroupName, streamName);
        PravegaSplitReader reader = createSplitReader(READER0, readerGroupName);
        assignSplit(reader, split);

        try (final EventStreamWriter<Integer> eventWriter = SETUP_UTILS.getIntegerWriter(streamName)) {
            AtomicBoolean exit = new AtomicBoolean();
            Thread t1 =
                    new Thread(
                            () -> {
                                int i = 0;
                                while (!exit.get()) {
                                    eventWriter.writeEvent(i++);
                                }
                            },
                            "nonExisting Pravega stream");
            t1.start();

            AtomicReference<RecordsWithSplitIds<EventRead<ByteBuffer>>> records = new AtomicReference<>();
            Thread t2 =
                    new Thread(
                            () -> records.set(reader.fetch()),
                            "testWakeUp-thread");
            t2.start();
            Thread.sleep(1000);

            reader.wakeUp();
            Thread.sleep(10);
            Assert.assertEquals(Whitebox.getInternalState(records.get(), "splitsIterator"), Collections.emptyIterator());
            exit.set(true);
            t1.join();
        }
        reader.close();
    }

    // ------------------

    private void assignSplitsAndFetchUntilFinish(
            PravegaSplitReader reader, PravegaSplit split, String streamName) throws Exception {
        assignSplit(reader, split);
        RecordsWithSplitIds<EventRead<ByteBuffer>> recordsBySplitIds;
        EventRead<ByteBuffer> eventRead;
        try (final EventStreamWriter<Integer> eventWriter = SETUP_UTILS.getIntegerWriter(streamName)) {
            int numEvents = 0;
            Set<String> finishedSplits = new HashSet<>();
            for (int i = 0; i < NUM_EVENTS; i++) {
                eventWriter.writeEvent(i);
            }

            while (finishedSplits.size() < 1) {
                recordsBySplitIds = reader.fetch();
                String splitId = recordsBySplitIds.nextSplit();
                while (splitId != null) {
                    while ((eventRead = recordsBySplitIds.nextRecordFromSplit()) != null) {
                        if (eventRead.getEvent() != null) {
                            numEvents++;
                        }
                    }
                    finishedSplits.add(splitId);
                    Assert.assertEquals(numEvents, NUM_EVENTS);
                    splitId = recordsBySplitIds.nextSplit();
                }
            }
        }
    }

    private void assignSplit(PravegaSplitReader reader, PravegaSplit split) {
        SplitsChange<PravegaSplit> splitsChange =
                new SplitsAddition<>(Collections.singletonList(split));
        reader.handleSplitsChanges(splitsChange);
    }

    private PravegaSplitReader createSplitReader(int subtaskId, String readerGroupName) throws Exception {
        return new PravegaSplitReader(
                SETUP_UTILS.getScope(),
                SETUP_UTILS.getClientConfig(),
                readerGroupName,
                subtaskId);
    }

    private static void createReaderGroup(String readerGroupName, String streamName) throws Exception {
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SETUP_UTILS.getScope(), SETUP_UTILS.getClientConfig());
        Stream stream = Stream.of(SETUP_UTILS.getScope(), streamName);
        readerGroupManager.createReaderGroup(readerGroupName, ReaderGroupConfig.builder().stream(stream).disableAutomaticCheckpoints().build());
        readerGroupManager.close();
    }
}
