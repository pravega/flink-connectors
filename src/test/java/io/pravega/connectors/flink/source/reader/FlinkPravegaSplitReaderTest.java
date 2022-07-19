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

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.source.split.PravegaSplit;
import io.pravega.connectors.flink.util.FlinkPravegaUtils;
import io.pravega.connectors.flink.utils.PravegaTestEnvironment;
import io.pravega.connectors.flink.utils.runtime.PravegaRuntime;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/** Unit tests for {@link PravegaSplitReader}. */
public class FlinkPravegaSplitReaderTest {
    private static final int NUM_PRAVEGA_SEGMENTS = 4;
    private static final int READER0 = 0;
    private static final int NUM_EVENTS = 100;

    private static final PravegaTestEnvironment PRAVEGA = new PravegaTestEnvironment(PravegaRuntime.container());

    @BeforeClass
    public static void setupPravega() throws Exception {
        PRAVEGA.startUp();
    }

    @AfterClass
    public static void tearDownPravega() throws Exception {
        PRAVEGA.tearDown();
    }

    @Test
    public void testHandleSplitChangesAndFetch() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        final String readerGroupName = FlinkPravegaUtils.generateRandomReaderGroupName();
        final PravegaSplit split = new PravegaSplit(readerGroupName, READER0);
        PRAVEGA.operator().createTestStream(streamName, NUM_PRAVEGA_SEGMENTS);
        createReaderGroup(readerGroupName, streamName);
        PravegaSplitReader reader = createSplitReader(READER0, readerGroupName);
        assignSplitsAndFetchUntilFinish(reader, split, streamName);
        reader.close();
    }

    @Test
    public void testCloseWithException() throws Exception {
        PravegaSplitReader reader = new BadPravegaSplitReader(
                "scope",
                null,
                "rg",
                1);
        Throwable thrown = Assert.assertThrows("close EventStreamReader failure", RuntimeException.class, reader::close);
        Assert.assertEquals(thrown.getSuppressed().length, 1);
        Assert.assertEquals(thrown.getSuppressed()[0].getMessage(), "close EventStreamClientFactory failure");
    }

    // ------------------

    private void assignSplitsAndFetchUntilFinish(
            PravegaSplitReader reader, PravegaSplit split, String streamName) throws Exception {
        assignSplit(reader, split);
        RecordsWithSplitIds<EventRead<ByteBuffer>> recordsBySplitIds;
        EventRead<ByteBuffer> eventRead;
        try (final EventStreamWriter<Integer> eventWriter = PRAVEGA.operator().getIntegerWriter(streamName)) {
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
                PRAVEGA.operator().getScope(),
                PRAVEGA.operator().getClientConfig(),
                readerGroupName,
                subtaskId);
    }

    private static void createReaderGroup(String readerGroupName, String streamName) throws Exception {
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(PRAVEGA.operator().getScope(), PRAVEGA.operator().getClientConfig());
        Stream stream = Stream.of(PRAVEGA.operator().getScope(), streamName);
        readerGroupManager.createReaderGroup(readerGroupName, ReaderGroupConfig.builder().stream(stream).disableAutomaticCheckpoints().build());
        readerGroupManager.close();
    }

    /**
     * A Pravega split reader subclass for test purposes. This class is used for testing negative cases, including
     * unsuccessful resources cleanup when closing split reader, and more future cases can be added through this class.
     */
    private static class BadPravegaSplitReader extends PravegaSplitReader {

        protected BadPravegaSplitReader(String scope, ClientConfig clientConfig, String readerGroupName, int subtaskId) {
            super(scope, clientConfig, readerGroupName, subtaskId);
        }

        @Override
        protected EventStreamClientFactory createEventStreamClientFactory(String readerGroupScope, ClientConfig clientConfig) {
            EventStreamClientFactory eventStreamClientFactory = mock(EventStreamClientFactory.class);
            doThrow(new RuntimeException("close EventStreamClientFactory failure")).when(eventStreamClientFactory).close();
            return eventStreamClientFactory;
        }

        @Override
        protected EventStreamReader<ByteBuffer> createEventStreamReader(String readerId, String readerGroupName) {
            EventStreamReader<ByteBuffer> eventStreamReader = mock(EventStreamReader.class);
            doThrow(new RuntimeException("close EventStreamReader failure")).when(eventStreamReader).close();
            return eventStreamReader;
        }
    }
}
