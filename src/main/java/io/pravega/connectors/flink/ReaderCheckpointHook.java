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

import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;

import javax.annotation.concurrent.GuardedBy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The hook executed in Flink's Checkpoint Coordinator that triggers and restores
 * checkpoints in on a Pravega ReaderGroup.
 */
@Slf4j
class ReaderCheckpointHook implements MasterTriggerRestoreHook<Checkpoint> {

    /** The prefix of checkpoint names */
    private static final String PRAVEGA_CHECKPOINT_NAME_PREFIX = "PVG-CHK-";

    /** Default thread pool size of the checkpoint scheduler */
    private static final int DEFAULT_CHECKPOINT_THREAD_POOL_SIZE = 3;

    // ------------------------------------------------------------------------

    /** The logical name of the operator. This is different from the (randomly generated)
     * reader group name, because it is used to identify the state in a checkpoint/savepoint
     * when resuming the checkpoint/savepoint with another job. */
    private final String hookUid;

    /** The serializer for Pravega checkpoints, to store them in Flink checkpoints */
    private final CheckpointSerializer checkpointSerializer;

    /** The reader group used to trigger and restore pravega checkpoints */
    private final ReaderGroup readerGroup;

    /** The timeout on the future returned by the 'initiateCheckpoint()' call */
    private final Time triggerTimeout;

    // The Pravega reader group config.
    private final ReaderGroupConfig readerGroupConfig;

    private final Object scheduledExecutorLock = new Object();

    // A long-lived thread pool for scheduling all checkpoint tasks
    @GuardedBy("scheduledExecutorLock")
    private ScheduledExecutorService scheduledExecutorService;

    ReaderCheckpointHook(String hookUid, ReaderGroup readerGroup, Time triggerTimeout, ReaderGroupConfig readerGroupConfig) {

        this.hookUid = checkNotNull(hookUid);
        this.readerGroup = checkNotNull(readerGroup);
        this.triggerTimeout = triggerTimeout;
        this.readerGroupConfig = readerGroupConfig;
        this.checkpointSerializer = new CheckpointSerializer();
    }

    // ------------------------------------------------------------------------

    @Override
    public String getIdentifier() {
        return this.hookUid;
    }

    @Override
    public CompletableFuture<Checkpoint> triggerCheckpoint(
            long checkpointId, long checkpointTimestamp, Executor executor) throws Exception {

        ensureScheduledExecutorExists();

        final String checkpointName = createCheckpointName(checkpointId);

        final CompletableFuture<Checkpoint> checkpointResult =
                this.readerGroup.initiateCheckpoint(checkpointName, scheduledExecutorService);

        // Add a timeout to the future, to prevent long blocking calls
        scheduledExecutorService.schedule(() -> checkpointResult.cancel(false), triggerTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

        return checkpointResult;
    }

    @Override
    public void restoreCheckpoint(long checkpointId, Checkpoint checkpoint) throws Exception {
        // checkpoint can be null when restoring from a savepoint that
        // did not include any state for that particular reader name
        if (checkpoint != null) {
             this.readerGroup.resetReaderGroup(ReaderGroupConfig
                    .builder()
                    .maxOutstandingCheckpointRequest(this.readerGroupConfig.getMaxOutstandingCheckpointRequest())
                    .groupRefreshTimeMillis(this.readerGroupConfig.getGroupRefreshTimeMillis())
                    .disableAutomaticCheckpoints()
                    .startFromCheckpoint(checkpoint)
                    .build());
        }
    }

    @Override
    public void reset() {
        // To avoid the data loss, reset the reader group using the reader config that was initially passed to the job.
        // This can happen when the job recovery happens after a failure but no checkpoint has been taken.
        log.info("resetting the reader group to initial state using the RG config {}", this.readerGroupConfig);
        this.readerGroup.resetReaderGroup(this.readerGroupConfig);
    }

    @Override
    public void close() {
        // close the reader group properly
        log.info("closing the reader group");
        this.readerGroup.close();

        synchronized (scheduledExecutorLock) {
            if (scheduledExecutorService != null ) {
                log.info("Closing Scheduled Executor for hook {}", hookUid);
                scheduledExecutorService.shutdownNow();
                scheduledExecutorService = null;
            }
        }
    }

    @Override
    public SimpleVersionedSerializer<Checkpoint> createCheckpointDataSerializer() {
        return this.checkpointSerializer;
    }

    // ------------------------------------------------------------------------
    //  utils
    // ------------------------------------------------------------------------

    private void ensureScheduledExecutorExists() {
        synchronized (scheduledExecutorLock) {
            if (scheduledExecutorService == null) {
                log.info("Creating Scheduled Executor for hook {}", hookUid);
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

    static long parseCheckpointId(String checkpointName) {
        checkArgument(checkpointName.startsWith(PRAVEGA_CHECKPOINT_NAME_PREFIX));

        try {
            return Long.parseLong(checkpointName.substring(PRAVEGA_CHECKPOINT_NAME_PREFIX.length()));
        } catch (NumberFormatException | IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(e);
        }
    }

    static String createCheckpointName(long checkpointId) {
        return PRAVEGA_CHECKPOINT_NAME_PREFIX + checkpointId;
    }

}
