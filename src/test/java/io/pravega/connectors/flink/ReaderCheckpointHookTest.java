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
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.CheckpointImpl;
import io.pravega.client.stream.impl.StreamCutImpl;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
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
        assertEquals(HOOK_UID, hook.getIdentifier());
        assertTrue(hook.createCheckpointDataSerializer() instanceof CheckpointSerializer);
    }

    @Test
    public void testTriggerCheckpoint() throws Exception {
        ReaderGroupConfig readerGroupConfig = mock(ReaderGroupConfig.class);
        ClientConfig clientConfig = mock(ClientConfig.class);
        CompletableFuture<Checkpoint> checkpointPromise = new CompletableFuture<>();
        TestableReaderCheckpointHook hook = new TestableReaderCheckpointHook(HOOK_UID, READER_GROUP_NAME, SCOPE, Time.minutes(1), clientConfig, readerGroupConfig);

        when(hook.readerGroup.initiateCheckpoint(anyString(), any())).thenReturn(checkpointPromise);
        CompletableFuture<Checkpoint> checkpointFuture = hook.triggerCheckpoint(1L, 1L, Executors.directExecutor());
        assertNotNull(checkpointFuture);
        verify(hook.readerGroup).initiateCheckpoint(anyString(), any());

        // complete the checkpoint promise
        Checkpoint expectedCheckpoint = mock(Checkpoint.class);
        checkpointPromise.complete(expectedCheckpoint);
        assertTrue(checkpointFuture.isDone());
        assertSame(expectedCheckpoint, checkpointFuture.get());
    }

    @Test
    public void testTriggerCheckpointTimeout() throws Exception {
        ReaderGroupConfig readerGroupConfig = mock(ReaderGroupConfig.class);
        ClientConfig clientConfig = mock(ClientConfig.class);
        CompletableFuture<Checkpoint> checkpointPromise = new CompletableFuture<>();

        TestableReaderCheckpointHook hook = new TestableReaderCheckpointHook(HOOK_UID, READER_GROUP_NAME, SCOPE, Time.minutes(1), clientConfig, readerGroupConfig);
        when(hook.readerGroup.initiateCheckpoint(anyString(), any())).thenReturn(checkpointPromise);

        CompletableFuture<Checkpoint> checkpointFuture = hook.triggerCheckpoint(1L, 1L, Executors.directExecutor());
        assertNotNull(checkpointFuture);
        verify(hook.readerGroup).initiateCheckpoint(anyString(), any());

        // invoke the timeout callback
        hook.invokeScheduledCallables();
        assertTrue(checkpointFuture.isCancelled());
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
        assertNull(hook.getScheduledExecutorService());
    }

    @Test
    public void testRestore() throws Exception {
        Checkpoint checkpoint = mock(Checkpoint.class);
        CheckpointImpl checkpointImpl = mock(CheckpointImpl.class);

        when(checkpoint.asImpl()).thenReturn(checkpointImpl);
        when(checkpointImpl.getPositions()).thenReturn(ImmutableMap.<Stream, StreamCut>builder()
                .put(Stream.of(SCOPE, "s1"), getStreamCut("s1"))
                .put(Stream.of(SCOPE, "s2"), getStreamCut("s2")).build());

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
                scheduledCallable = a.getArgumentAt(0, Callable.class);
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
        ImmutableMap<Segment, Long> positions = ImmutableMap.<Segment, Long>builder().put(new Segment(SCOPE,
                streamName, 0), offset).build();
        return new StreamCutImpl(Stream.of(SCOPE, streamName), positions);
    }
}
