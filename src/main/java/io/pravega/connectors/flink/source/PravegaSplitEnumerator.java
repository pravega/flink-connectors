/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink.source;

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.base.source.event.NoMoreSplitsEvent;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class PravegaSplitEnumerator implements SplitEnumerator<PravegaSplit, Checkpoint> {
    private final SplitEnumeratorContext<PravegaSplit> enumContext;
    private ReaderGroup readerGroup;
    private ReaderGroupManager readerGroupManager;
    private ClientConfig clientConfig;
    private ReaderGroupConfig readerGroupConfig;
    private String scope;
    private String readerGroupName;
    private AtomicInteger checkpointId;
    private List<PravegaSplit> splits;
    private Checkpoint checkpoint;

    /** Default thread pool size of the checkpoint scheduler */
    private static final int DEFAULT_CHECKPOINT_THREAD_POOL_SIZE = 3;
    private final Object scheduledExecutorLock = new Object();

    // A long-lived thread pool for scheduling all checkpoint tasks
    @GuardedBy("scheduledExecutorLock")
    private ScheduledExecutorService scheduledExecutorService;

    public PravegaSplitEnumerator(
            SplitEnumeratorContext<PravegaSplit> context,
            String scope,
            String readerGroupName,
            ClientConfig clientConfig,
            ReaderGroupConfig readerGroupConfig,
            Checkpoint checkpoint) {
        this.enumContext = context;
        this.scope = scope;
        this.readerGroupName = readerGroupName;
        this.clientConfig = clientConfig;
        this.readerGroupConfig = readerGroupConfig;
        this.splits = new ArrayList<>();
        for (int i = 0; i < enumContext.currentParallelism(); i++) {
            splits.add(new PravegaSplit(readerGroupName, i));
        }
        this.checkpointId = new AtomicInteger();
        this.checkpoint = checkpoint;
    }

    @Override
    public void start() {

        log.info("Creating reader group: {}/{}.", this.scope, this.readerGroupName);

        if (this.readerGroupManager == null) {
            this.readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig);
        }

        if (this.readerGroup == null) {
            readerGroupManager.createReaderGroup(this.readerGroupName, readerGroupConfig);
            this.readerGroup = readerGroupManager.getReaderGroup(this.readerGroupName);
        }

        if (this.checkpoint != null) {
            log.info("Recover from checkpoint: {}", checkpoint.getName());
            this.readerGroup.resetReaderGroup(ReaderGroupConfig
                    .builder()
                    .maxOutstandingCheckpointRequest(this.readerGroupConfig.getMaxOutstandingCheckpointRequest())
                    .groupRefreshTimeMillis(this.readerGroupConfig.getGroupRefreshTimeMillis())
                    .disableAutomaticCheckpoints()
                    .startFromCheckpoint(this.checkpoint)
                    .build());
        }
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
    }

    @Override
    public void addReader(int subtaskId) {
        log.info("Add reader called");
        if (enumContext.registeredReaders().size() == enumContext.currentParallelism() && splits.size() > 0) {
            int numReaders = enumContext.registeredReaders().size();
            Map<Integer, List<PravegaSplit>> assignment = new HashMap<>();
            for (int i = 0; i < numReaders; i++) {
                assignment.put(i, Collections.singletonList(splits.get(i)));
            }
            enumContext.assignSplits(new SplitsAssignment<>(assignment));
            splits.clear();
            for (int i = 0; i < numReaders; i++) {
                enumContext.sendEventToSourceReader(i, new NoMoreSplitsEvent());
            }
        }
    }

    @Override
    public Checkpoint snapshotState() throws Exception {
        ensureScheduledExecutorExists();

        final String checkpointName = createCheckpointName(checkpointId.getAndIncrement());

        log.info("Initiate checkpoint {}", checkpointName);
        final CompletableFuture<Checkpoint> checkpointResult =
                this.readerGroup.initiateCheckpoint(checkpointName, scheduledExecutorService);

        return checkpointResult.get(5, TimeUnit.SECONDS);
    }

    @Override
    public void close() throws IOException {
        readerGroup.close();
//        readerGroupManager.deleteReaderGroup(readerGroupName);
        readerGroupManager.close();
    }

    @Override
    public void addSplitsBack(List<PravegaSplit> splits, int subtaskId) {
        log.info("Call addSplitsBack");
        throw new RuntimeException("Intentional");
    }


    // -------------

    private void ensureScheduledExecutorExists() {
        synchronized (scheduledExecutorLock) {
            if (scheduledExecutorService == null) {
                log.info("Creating Scheduled Executor");
                scheduledExecutorService = createScheduledExecutorService();
            }
        }
    }

    protected ScheduledExecutorService createScheduledExecutorService() {
        return Executors.newScheduledThreadPool(DEFAULT_CHECKPOINT_THREAD_POOL_SIZE);
    }

    protected ScheduledExecutorService getScheduledExecutorService() {
        return this.scheduledExecutorService;
    }

    static String createCheckpointName(long checkpointId) {
        return "PVG-CHK-" + checkpointId;
    }
}
