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
package io.pravega.connectors.flink;

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.CheckpointImpl;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.connectors.flink.serialization.CheckpointSerializer;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReaderCheckpointHookTest {

    private static final String HOOK_UID = "test";
    private static final String SCOPE = "scope";
    private static final String READER_GROUP_NAME = "reader";

    @Test
    public void testConstructor() throws Exception {
        ReaderGroupConfig readerGroupConfig = mock(ReaderGroupConfig.class);
        ClientConfig clientConfig = mock(ClientConfig.class);
        TestableReaderCheckpointHook hook = new TestableReaderCheckpointHook(HOOK_UID, READER_GROUP_NAME, SCOPE, Time.minutes(1), clientConfig, readerGroupConfig);
        assertThat(hook.getIdentifier()).isEqualTo(HOOK_UID);
        assertThat(hook.createCheckpointDataSerializer() instanceof CheckpointSerializer).isTrue();
    }

    @Test
    public void testTriggerCheckpoint() throws Exception {
        ReaderGroupConfig readerGroupConfig = mock(ReaderGroupConfig.class);
        ClientConfig clientConfig = mock(ClientConfig.class);
        CompletableFuture<Checkpoint> checkpointPromise = new CompletableFuture<>();
        TestableReaderCheckpointHook hook = new TestableReaderCheckpointHook(HOOK_UID, READER_GROUP_NAME, SCOPE, Time.minutes(1), clientConfig, readerGroupConfig);

        when(hook.readerGroup.initiateCheckpoint(anyString())).thenReturn(checkpointPromise);
        CompletableFuture<Checkpoint> checkpointFuture = hook.triggerCheckpoint(1L, 1L, Executors.directExecutor());
        assertThat(checkpointFuture).isNotNull();
        verify(hook.readerGroup).initiateCheckpoint(anyString());

        // complete the checkpoint promise
        Checkpoint expectedCheckpoint = mock(Checkpoint.class);
        checkpointPromise.complete(expectedCheckpoint);
        assertThat(checkpointFuture.isDone()).isTrue();
        assertThat(checkpointFuture.get()).isSameAs(expectedCheckpoint);
    }

    @Test
    public void testTriggerCheckpointTimeout() throws Exception {
        ReaderGroupConfig readerGroupConfig = mock(ReaderGroupConfig.class);
        ClientConfig clientConfig = mock(ClientConfig.class);
        CompletableFuture<Checkpoint> checkpointPromise = new CompletableFuture<>();

        TestableReaderCheckpointHook hook = new TestableReaderCheckpointHook(HOOK_UID, READER_GROUP_NAME, SCOPE, Time.minutes(1), clientConfig, readerGroupConfig);
        when(hook.readerGroup.initiateCheckpoint(anyString())).thenReturn(checkpointPromise);

        CompletableFuture<Checkpoint> checkpointFuture = hook.triggerCheckpoint(1L, 1L, Executors.directExecutor());
        assertThat(checkpointFuture).isNotNull();
        verify(hook.readerGroup).initiateCheckpoint(anyString());

        // invoke the timeout callback
        hook.invokeScheduledCallables();
        assertThat(checkpointFuture.isCancelled()).isTrue();
    }

    @Test
    public void testReset() {
        ReaderGroupConfig readerGroupConfig = mock(ReaderGroupConfig.class);
        ClientConfig clientConfig = mock(ClientConfig.class);
        TestableReaderCheckpointHook hook = new TestableReaderCheckpointHook(HOOK_UID, READER_GROUP_NAME, SCOPE, Time.minutes(1), clientConfig, readerGroupConfig);
        hook.reset();
        verify(hook.readerGroup).resetReaderGroup(readerGroupConfig);
    }

    @Test
    public void testClose() {
        ReaderGroupConfig readerGroupConfig = mock(ReaderGroupConfig.class);
        ClientConfig clientConfig = mock(ClientConfig.class);
        TestableReaderCheckpointHook hook = new TestableReaderCheckpointHook(HOOK_UID, READER_GROUP_NAME, SCOPE, Time.minutes(1), clientConfig, readerGroupConfig);
        hook.close();
        verify(hook.readerGroup).close();
        verify(hook.readerGroupManager).close();
        assertThat(hook.getScheduledExecutorService()).isNull();
    }

    @Test
    public void testRestore() throws Exception {
        Checkpoint checkpoint = mock(Checkpoint.class);
        CheckpointImpl checkpointImpl = mock(CheckpointImpl.class);

        when(checkpoint.asImpl()).thenReturn(checkpointImpl);
        when(checkpointImpl.getPositions()).thenReturn(new HashMap<Stream, StreamCut>() {{
            put(Stream.of(SCOPE, "s1"), getStreamCut("s1"));
            put(Stream.of(SCOPE, "s2"), getStreamCut("s2"));
        }});

        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .disableAutomaticCheckpoints()
                .startFromCheckpoint(checkpoint)
                .build();

        ClientConfig clientConfig = mock(ClientConfig.class);

        TestableReaderCheckpointHook hook = new TestableReaderCheckpointHook(HOOK_UID, READER_GROUP_NAME, SCOPE, Time.minutes(1), clientConfig, readerGroupConfig);

        hook.restoreCheckpoint(1L, checkpoint);

        verify(hook.readerGroup).resetReaderGroup(any(ReaderGroupConfig.class));
    }

    static class TestableReaderCheckpointHook extends ReaderCheckpointHook {
        private Callable<Void> scheduledCallable;

        @SuppressWarnings("unchecked")
        TestableReaderCheckpointHook(String hookUid, String readerGroupName,  String readerGroupScope, Time triggerTimeout, ClientConfig clientConfig, ReaderGroupConfig readerGroupConfig) {
            super(hookUid, readerGroupName, readerGroupScope, triggerTimeout, clientConfig, readerGroupConfig);
        }

        @Override
        protected void initializeReaderGroup(String readerGroupName, String readerGroupScope, ClientConfig clientConfig) {
            this.readerGroup = mock(ReaderGroup.class);
            this.readerGroupManager = mock(ReaderGroupManager.class);
        }

        @Override
        protected ScheduledExecutorService createScheduledExecutorService() {
            ScheduledExecutorService newScheduledExecutor = mock(ScheduledExecutorService.class);
            when(newScheduledExecutor.schedule(any(Callable.class), anyLong(), any())).thenAnswer(a -> {
                scheduledCallable = a.getArgument(0, Callable.class);
                return null;
            });

            return newScheduledExecutor;
        }

        public void invokeScheduledCallables() throws Exception {
            if (scheduledCallable != null) {
                scheduledCallable.call();
            }
        }
    }

    private StreamCut getStreamCut(String streamName) {
        return getStreamCut(streamName, 10L);
    }

    private StreamCut getStreamCut(String streamName, long offset) {
        HashMap<Segment, Long> positions = new HashMap<>();
        positions.put(new Segment(SCOPE, streamName, 0), offset);
        return new StreamCutImpl(Stream.of(SCOPE, streamName), positions);
    }
}
