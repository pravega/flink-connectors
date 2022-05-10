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

package io.pravega.connectors.flink.source.enumerator;

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.source.split.PravegaSplit;
import io.pravega.connectors.flink.util.FlinkPravegaUtils;
import io.pravega.connectors.flink.utils.SetupUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.mock.Whitebox;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/** Unit tests for {@link PravegaSplitEnumerator}. */
public class FlinkPravegaSplitEnumeratorTest {
    private static final int NUM_SUBTASKS = 2;
    private static final int NUM_PRAVEGA_SEGMENTS = 4;

    private static final int READER0 = 0;
    private static final int READER1 = 1;
    /** Setup utility */
    private static final SetupUtils SETUP_UTILS = new SetupUtils();

    // ------------------------------------------------------------------------

    @BeforeClass
    public static void setup() throws Exception {
        SETUP_UTILS.startAllServices();

    }

    @AfterClass
    public static void tearDown() throws Exception {
        SETUP_UTILS.stopAllServices();
    }

    @Test
    public void testAddReader() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        final String readerGroupName = FlinkPravegaUtils.generateRandomReaderGroupName();
        SETUP_UTILS.createTestStream(streamName, NUM_PRAVEGA_SEGMENTS);
        MockSplitEnumeratorContext<PravegaSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS);

        try (PravegaSplitEnumerator enumerator = createEnumerator(context, streamName, readerGroupName)) {
            // start enumerator
            enumerator.start();

            // register reader 0 and reader 1
            context.registerReader(new ReaderInfo(READER0, "location 0"));
            enumerator.addReader(READER0);
            context.registerReader(new ReaderInfo(READER1, "location 0"));
            enumerator.addReader(READER1);

            Assert.assertEquals(context.getSplitsAssignmentSequence().size(), 2);
            Assert.assertEquals(context.getSplitsAssignmentSequence().get(0).assignment().size(), 1);
            Assert.assertEquals(context.getSplitsAssignmentSequence().get(0).assignment().get(READER0).size(), 1);
            Assert.assertEquals(
                    context.getSplitsAssignmentSequence().get(0).assignment().get(READER0).get(0).getSubtaskId(), READER0);
            Assert.assertEquals(
                    context.getSplitsAssignmentSequence().get(1).assignment().get(READER1).get(0).getSubtaskId(), READER1);
            Assert.assertEquals(
                    context.getSplitsAssignmentSequence().get(0).assignment().get(READER0).get(0).getReaderGroupName(),
                    readerGroupName);
            Assert.assertEquals(
                    context.getSplitsAssignmentSequence().get(1).assignment().get(READER1).get(0).getReaderGroupName(),
                    readerGroupName);
        }
    }

    @Test
    public void testSnapshotState() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        final String readerGroupName = FlinkPravegaUtils.generateRandomReaderGroupName();
        SETUP_UTILS.createTestStream(streamName, NUM_PRAVEGA_SEGMENTS);
        MockSplitEnumeratorContext<PravegaSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS);

        final PravegaSplitEnumerator enumerator = createEnumerator(context, streamName, readerGroupName);
        // start enumerator
        enumerator.start();

        final Checkpoint checkpoint = enumerator.snapshotState(0);
        Assert.assertNotNull(checkpoint);
    }

    @Test
    public void testAddSplitsBack() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        final String readerGroupName = FlinkPravegaUtils.generateRandomReaderGroupName();
        final PravegaSplit split = new PravegaSplit(readerGroupName, READER0);
        SETUP_UTILS.createTestStream(streamName, NUM_PRAVEGA_SEGMENTS);
        MockSplitEnumeratorContext<PravegaSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
        try (PravegaSplitEnumerator enumerator = createEnumerator(context, streamName, readerGroupName)) {
            // start enumerator
            enumerator.start();

            // register reader 0 and reader 1
            context.registerReader(new ReaderInfo(READER0, "location 0"));
            enumerator.addReader(READER0);
            context.registerReader(new ReaderInfo(READER1, "location 0"));
            enumerator.addReader(READER1);

            try {
                enumerator.addSplitsBack(Collections.singletonList(split), READER0);
                Assert.fail("Expected a RuntimeException to be thrown");
            } catch (RuntimeException e) {
                Assert.assertEquals(e.getMessage(), "triggering global failure");
            }
        }
    }

    @Test
    public void testReaderGroup() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        final String readerGroupName = FlinkPravegaUtils.generateRandomReaderGroupName();
        SETUP_UTILS.createTestStream(streamName, NUM_PRAVEGA_SEGMENTS);
        MockSplitEnumeratorContext<PravegaSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS);

        try (PravegaSplitEnumerator enumerator = createEnumerator(context, streamName, readerGroupName)) {
            enumerator.start();

            ReaderGroupManager readerGroupManager =
                    (ReaderGroupManager) Whitebox.getInternalState(enumerator, "readerGroupManager");
            Assert.assertNotNull(readerGroupManager);
            String scope = (String) Whitebox.getInternalState(readerGroupManager, "scope");
            Assert.assertNotNull(scope);
            Assert.assertEquals(scope, SETUP_UTILS.getScope());

            ReaderGroup readerGroup =
                    (ReaderGroup) Whitebox.getInternalState(enumerator, "readerGroup");
            Assert.assertNotNull(readerGroup);
            Assert.assertEquals(readerGroup.getGroupName(), readerGroupName);
            Assert.assertEquals(readerGroup.getScope(), SETUP_UTILS.getScope());
        }
    }

    @Test
    public void testCloseWithException() throws Exception {
        PravegaSplitEnumerator enumerator = createTestableEnumerator();
        enumerator.start();
        Throwable thrown = Assert.assertThrows("close ReaderGroupManager failure", IOException.class, enumerator::close);
    }

    private PravegaSplitEnumerator createEnumerator(MockSplitEnumeratorContext<PravegaSplit> enumContext, String streamName,
                                                    String readerGroupName) {
        Stream stream = Stream.of(SETUP_UTILS.getScope(), streamName);
        return new PravegaSplitEnumerator(
                enumContext,
                SETUP_UTILS.getScope(),
                readerGroupName,
                SETUP_UTILS.getClientConfig(),
                ReaderGroupConfig.builder().stream(stream).build(),
                null);
    }

    private PravegaSplitEnumerator createTestableEnumerator() {
        return new TestablePravegaSplitEnumerator(
                null,
                "scope",
                "rg",
                null,
                null,
                null);
    }

    /**
     * A Pravega split enumerator subclass for test purposes.
     */
    private static class TestablePravegaSplitEnumerator extends PravegaSplitEnumerator {

        protected TestablePravegaSplitEnumerator(SplitEnumeratorContext<PravegaSplit> context, String scope,
                                                 String readerGroupName, ClientConfig clientConfig,
                                                 ReaderGroupConfig readerGroupConfig, Checkpoint checkpoint) {
            super(context, scope, readerGroupName, clientConfig, readerGroupConfig, checkpoint);
        }

        @Override
        protected ReaderGroupManager createReaderGroupManager() {
            ReaderGroupManager readerGroupManager = mock(ReaderGroupManager.class);
            doThrow(new RuntimeException("close ReaderGroupManager failure")).when(readerGroupManager).close();
            return readerGroupManager;
        }

        @Override
        protected ReaderGroup createReaderGroup() {
            ReaderGroup readerGroup = mock(ReaderGroup.class);
            doThrow(new RuntimeException("close ReaderGroup failure")).when(readerGroup).close();
            return readerGroup;
        }
    }
}
