/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink.source.enumerator;

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.connectors.flink.source.reader.PravegaSourceReader;
import io.pravega.connectors.flink.source.split.PravegaSplit;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;

import javax.annotation.Nullable;
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
    private final ClientConfig clientConfig;
    private final ReaderGroupConfig readerGroupConfig;
    private final String scope;
    private final String readerGroupName;
    private final List<PravegaSplit> splits;
    private Checkpoint checkpoint;
    private long checkpointId;
    private boolean isRecovered;

    /** Default thread pool size of the checkpoint scheduler */
    private static final int DEFAULT_CHECKPOINT_THREAD_POOL_SIZE = 3;

    // A long-lived thread pool for scheduling all checkpoint tasks
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
        this.checkpoint = checkpoint;
        if (checkpoint != null) {
            this.checkpointId = getCheckpointId(checkpoint.getName());
        }
        this.scheduledExecutorService = Executors.newScheduledThreadPool(DEFAULT_CHECKPOINT_THREAD_POOL_SIZE);
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
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {

    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
    }

    @Override
    public void addReader(int subtaskId) {
        if (enumContext.registeredReaders().size() == enumContext.currentParallelism() && splits.size() > 0) {
            int numReaders = enumContext.registeredReaders().size();
            Map<Integer, List<PravegaSplit>> assignment = new HashMap<>();
            for (int i = 0; i < numReaders; i++) {
                assignment.put(i, Collections.singletonList(splits.get(i)));
            }
            enumContext.assignSplits(new SplitsAssignment<>(assignment));
            splits.clear();
        }
    }

    @Override
    public Checkpoint snapshotState() throws Exception {
        checkpointId++;
        final String checkpointName = createCheckpointName(checkpointId);

        log.info("Initiate checkpoint {}", checkpointName);
        final CompletableFuture<Checkpoint> checkpointResult =
                this.readerGroup.initiateCheckpoint(checkpointName, scheduledExecutorService);
        try {
            this.checkpoint = checkpointResult.get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("Pravega checkpoint met error.", e);
            this.checkpoint = null;
        }

        for (int i = 0; i < enumContext.currentParallelism(); i++) {
            enumContext.sendEventToSourceReader(i, new CheckpointEvent(checkpointId));
        }

        return checkpoint;
    }

    @Override
    public void close() throws IOException {
        readerGroup.close();
        readerGroupManager.close();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        log.info("complete checkpoint {}", checkpointId);
        this.checkpointId = checkpointId;
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {

    }

    @Override
    public void addSplitsBack(List<PravegaSplit> splits, int subtaskId) {
        log.info("Call addSplitsBack {} : {}", splits.size(), subtaskId);
        if (!isRecovered) {
            isRecovered = true;
            throw new RuntimeException("hahaha");
        } else {
            log.info("huhuhu");
        }
    }


    // -------------


    static String createCheckpointName(long checkpointId) {
        return "PVG-CHK-" + checkpointId;
    }

    public static long getCheckpointId(String checkpointName) {
        return Long.parseLong(checkpointName.substring(8));
    }

    public static class CheckpointEvent implements SourceEvent {
        private final long checkpointId;

        private CheckpointEvent(long checkpointId) {
            this.checkpointId = checkpointId;
        }

        public long getCheckpointId() {
            return checkpointId;
        }
    }
}
