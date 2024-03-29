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
import io.pravega.connectors.flink.utils.PravegaTestEnvironment;
import io.pravega.connectors.flink.utils.runtime.PravegaRuntime;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.mock.Whitebox;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/** Unit tests for {@link PravegaSplitEnumerator}. */
public class FlinkPravegaSplitEnumeratorTest {
    private static final int NUM_SUBTASKS = 2;
    private static final int NUM_PRAVEGA_SEGMENTS = 4;

    private static final int READER0 = 0;
    private static final int READER1 = 1;

    private static final PravegaTestEnvironment PRAVEGA = new PravegaTestEnvironment(PravegaRuntime.container());

    @BeforeAll
    public static void setupPravega() throws Exception {
        PRAVEGA.startUp();
    }

    @AfterAll
    public static void tearDownPravega() throws Exception {
        PRAVEGA.tearDown();
    }

    @Test
    public void testAddReader() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        final String readerGroupName = FlinkPravegaUtils.generateRandomReaderGroupName();
        PRAVEGA.operator().createTestStream(streamName, NUM_PRAVEGA_SEGMENTS);
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

            assertThat(context.getSplitsAssignmentSequence().size()).isEqualTo(2);
            assertThat(context.getSplitsAssignmentSequence().get(0).assignment().size()).isEqualTo(1);
            assertThat(context.getSplitsAssignmentSequence().get(0).assignment().get(READER0).size()).isEqualTo(1);
            assertThat(context.getSplitsAssignmentSequence().get(0).assignment().get(READER0).get(0).getSubtaskId())
                    .isEqualTo(READER0);
            assertThat(context.getSplitsAssignmentSequence().get(1).assignment().get(READER1).get(0).getSubtaskId())
                    .isEqualTo(READER1);
            assertThat(
                    context.getSplitsAssignmentSequence().get(0).assignment().get(READER0).get(0).getReaderGroupName())
                    .isEqualTo(readerGroupName);
            assertThat(
                    context.getSplitsAssignmentSequence().get(1).assignment().get(READER1).get(0).getReaderGroupName())
                    .isEqualTo(readerGroupName);
        }
    }

    @Test
    public void testSnapshotState() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        final String readerGroupName = FlinkPravegaUtils.generateRandomReaderGroupName();
        PRAVEGA.operator().createTestStream(streamName, NUM_PRAVEGA_SEGMENTS);
        MockSplitEnumeratorContext<PravegaSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS);

        final PravegaSplitEnumerator enumerator = createEnumerator(context, streamName, readerGroupName);
        // start enumerator
        enumerator.start();

        final Checkpoint checkpoint = enumerator.snapshotState(0);
        assertThat(checkpoint).isNotNull();
    }

    @Test
    public void testAddSplitsBack() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        final String readerGroupName = FlinkPravegaUtils.generateRandomReaderGroupName();
        final PravegaSplit split = new PravegaSplit(readerGroupName, READER0);
        PRAVEGA.operator().createTestStream(streamName, NUM_PRAVEGA_SEGMENTS);
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
                fail("Expected a RuntimeException to be thrown");
            } catch (RuntimeException e) {
                assertThat(e.getMessage()).isEqualTo("triggering global failure");
            }
        }
    }

    @Test
    public void testReaderGroup() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        final String readerGroupName = FlinkPravegaUtils.generateRandomReaderGroupName();
        PRAVEGA.operator().createTestStream(streamName, NUM_PRAVEGA_SEGMENTS);
        MockSplitEnumeratorContext<PravegaSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS);

        try (PravegaSplitEnumerator enumerator = createEnumerator(context, streamName, readerGroupName)) {
            enumerator.start();

            ReaderGroupManager readerGroupManager =
                    (ReaderGroupManager) Whitebox.getInternalState(enumerator, "readerGroupManager");
            assertThat(readerGroupManager).isNotNull();
            String scope = (String) Whitebox.getInternalState(readerGroupManager, "scope");
            assertThat(scope).isNotNull();
            assertThat(scope).isEqualTo(PRAVEGA.operator().getScope());

            ReaderGroup readerGroup =
                    (ReaderGroup) Whitebox.getInternalState(enumerator, "readerGroup");
            assertThat(readerGroup).isNotNull();
            assertThat(readerGroup.getGroupName()).isEqualTo(readerGroupName);
            assertThat(readerGroup.getScope()).isEqualTo(PRAVEGA.operator().getScope());
        }
    }

    @Test
    public void testCloseWithException() throws Exception {
        PravegaSplitEnumerator enumerator = new BadPravegaSplitEnumerator(
                null,
                "scope",
                "rg",
                null,
                null,
                null);
        enumerator.start();
        assertThatThrownBy(enumerator::close).isInstanceOf(IOException.class)
                .hasMessageContaining("close ReaderGroupManager failure");
    }

    private PravegaSplitEnumerator createEnumerator(MockSplitEnumeratorContext<PravegaSplit> enumContext, String streamName,
                                                    String readerGroupName) {
        Stream stream = Stream.of(PRAVEGA.operator().getScope(), streamName);
        return new PravegaSplitEnumerator(
                enumContext,
                PRAVEGA.operator().getScope(),
                readerGroupName,
                PRAVEGA.operator().getClientConfig(),
                ReaderGroupConfig.builder().stream(stream).build(),
                null);
    }

    /**
     * A Pravega split enumerator subclass for test purposes. This class is used for testing negative cases, including
     * unsuccessful resources cleanup when closing enumerator, and more future cases can be added through this class.
     */
    private static class BadPravegaSplitEnumerator extends PravegaSplitEnumerator {

        protected BadPravegaSplitEnumerator(SplitEnumeratorContext<PravegaSplit> context, String scope,
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
