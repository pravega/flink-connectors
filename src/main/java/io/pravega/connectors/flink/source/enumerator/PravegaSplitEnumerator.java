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
import io.pravega.connectors.flink.source.split.PravegaSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** The enumerator class for Pravega source. */
public class PravegaSplitEnumerator implements SplitEnumerator<PravegaSplit, Checkpoint> {
    /** Default thread pool size of the checkpoint scheduler */
    private static final int DEFAULT_CHECKPOINT_THREAD_POOL_SIZE = 3;

    private static final Logger LOG = LoggerFactory.getLogger(PravegaSplitEnumerator.class);
    private final SplitEnumeratorContext<PravegaSplit> enumContext;

    // The readergroup to coordinate readers.
    private ReaderGroup readerGroup;

    // The readergroup manager to manager reader groups.
    private ReaderGroupManager readerGroupManager;

    // The Pravega client config
    private final ClientConfig clientConfig;

    // The Pravega reader group config.
    private final ReaderGroupConfig readerGroupConfig;

    // The scope name of the reader group.
    private final String scope;

    // The readergroup name to coordinate the parallel readers.
    private final String readerGroupName;

    // Pravega splits list.
    private final List<PravegaSplit> splits;

    // Pravega checkpoint for enymerator.
    private Checkpoint checkpoint;

    // Flag to indicate whether it's already in the recovering process
    // so that we don't throw multiple redundant exceptions.
    private boolean isRecovered;

    // A long-lived thread pool for scheduling all checkpoint tasks
    private ScheduledExecutorService scheduledExecutorService;

    /**
     * Creates a new Pravega Split Enumerator instance which can connect to a
     * Pravega reader group with the pravega stream.
     * The Enumerator is a single instance on Flink jobmanager. It is the "brain" of the source to initialize
     * the reader group when it starts, then discover and assign the subtasks.
     *
     * @param context              The Pravega Split Enumeratior context.
     * @param scope                The reader group scope name.
     * @param readerGroupName      The reader group name.
     * @param clientConfig         The Pravega client configuration.
     * @param readerGroupConfig    The Pravega reader group configuration.
     * @param checkpoint           The Pravega checkpoint.
     */
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
        this.scheduledExecutorService = Executors.newScheduledThreadPool(DEFAULT_CHECKPOINT_THREAD_POOL_SIZE);
    }

    // initiate reader group manager, reader group and reset the group to the checkpoint position if checkpoint isn't null
    @Override
    public void start() {
        LOG.info("Starting the PravegaSplitEnumerator for reader group: {}/{}.", this.scope, this.readerGroupName);

        if (this.readerGroupManager == null) {
            this.readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig);
        }
        if (this.readerGroup == null) {
            this.readerGroupManager.createReaderGroup(this.readerGroupName, readerGroupConfig);
            this.readerGroup = this.readerGroupManager.getReaderGroup(this.readerGroupName);
        }
        if (this.checkpoint != null) {
            LOG.info("Recover from checkpoint: {}", checkpoint.getName());
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
        // the Pravega source pushes splits eagerly, rather than act upon split requests
    }

    // check with the current parallelism and update the assignment (always one-to-one mapping)
    @Override
    public void addReader(int subtaskId) {
        if (enumContext.registeredReaders().size() == enumContext.currentParallelism() && splits.size() > 0) {
            LOG.debug("Adding reader {} to PravegaSplitEnumerator for reader group: {}/{}.",
                    subtaskId,
                    scope,
                    readerGroupName);
            int numReaders = enumContext.registeredReaders().size();
            Map<Integer, List<PravegaSplit>> assignment = new HashMap<>();
            for (int i = 0; i < numReaders; i++) {
                assignment.put(i, Collections.singletonList(splits.get(i)));
            }
            enumContext.assignSplits(new SplitsAssignment<>(assignment));
            splits.clear();
        }
    }

    // deal with the data recovery, it will call readerGroup::initiateCheckpoint to get a checkpoint for recovery.
    // this call will be handled in another thread pool instead of the split enumerator thread.
    @Override
    public Checkpoint snapshotState(long chkPtID) throws Exception {
        final String checkpointName = createCheckpointName(chkPtID);

        LOG.info("Initiate checkpoint {}", checkpointName);
        final CompletableFuture<Checkpoint> checkpointResult =
                this.readerGroup.initiateCheckpoint(checkpointName, scheduledExecutorService);
        try {
            this.checkpoint = checkpointResult.get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.error("Pravega checkpoint met error.", e);
            this.checkpoint = null;
        }

        return checkpoint;
    }

    @Override
    public void close() throws IOException {
        readerGroup.close();
        readerGroupManager.close();
    }

    // This method is called when it tries to "regionally" recover from a failure to assign unassigned splits. We will
    // throw an intentional exception here to trigger a global recovery since Pravega since Pravega
    // doesn't have a mechanism to recover from partial failure. This will shutdown the reader group,
    // recreate it and recover it from the latest checkpoint.
    @Override
    public void addSplitsBack(List<PravegaSplit> splits, int subtaskId) {
        LOG.info("Call addSplitsBack {} : {}", splits.size(), subtaskId);

        if (!isRecovered) {
            isRecovered = true;
            throw new RuntimeException("triggering global failure");
        }
    }

    // -------------

    static String createCheckpointName(long checkpointId) {
        return "PVG-CHK-" + checkpointId;
    }
}
