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
import io.pravega.connectors.flink.source.split.PravegaSplit;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The enumerator class for Pravega source. It is a single instance on Flink jobmanager.
 * It is the "brain" of the source to initialize the reader group when it starts, then discover and assign the subtasks.
 *
 * <p>The {@link PravegaSplitEnumerator} will assign splits to source readers. Pravega source pushes splits eagerly so that
 * the enumerator will create a source reader and assign one split(Pravega reader) to it.
 * One Pravega Source reader is only mapped to one Pravega Split as design.
 *
 * <p>We triggers and restores checkpoints on a Pravega ReaderGroup along with Flink checkpoints for dealing with failure.
 * Due to Pravega's design, we don't have a mechanism to recover from a single reader currently. We will perform a full failover
 * both when Split Enumerator fails or Source Reader fails. The Split Enumerator will be restored to to its last successful checkpoint.
 *
 * */
@Internal
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

    // The number of Pravega source readers.
    private int numReaders = 0;

    // Pravega checkpoint for enumerator.
    private Checkpoint checkpoint;

    // Flag to indicate whether it's already in the recovering process
    // so that we don't throw multiple redundant exceptions.
    private boolean isRecovered;

    // A long-lived thread pool for scheduling all checkpoint tasks
    private ScheduledExecutorService scheduledExecutorService;

    /**
     * Creates a new Pravega Split Enumerator instance which can connect to a
     * Pravega reader group with the pravega stream.
     *
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
        this.checkpoint = checkpoint;
        this.scheduledExecutorService = Executors.newScheduledThreadPool(DEFAULT_CHECKPOINT_THREAD_POOL_SIZE);
    }

    // initiate reader group manager, reader group and reset the group to the checkpoint position if checkpoint isn't null
    @Override
    public void start() {
        LOG.info("Starting the PravegaSplitEnumerator for reader group: {}/{}.", this.scope, this.readerGroupName);

        if (this.readerGroupManager == null) {
            this.readerGroupManager = createReaderGroupManager();
        }
        if (this.readerGroup == null) {
            this.readerGroup = createReaderGroup();
        }
        if (this.checkpoint != null) {
            LOG.info("Resetting reader group {} from checkpoint: {}", this.readerGroupName, checkpoint.getName());
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

    // Check with the current parallelism and add the assignment (always one-to-one mapping).
    // One Pravega Source reader is mapped to one Pravega Split which represents an EventStreamReader so that
    // we can just assign the split to reader.
    @Override
    public void addReader(int subtaskId) {
        if (numReaders++ < enumContext.currentParallelism()) {
            LOG.info("Adding reader {} to PravegaSplitEnumerator for reader group: {}/{}.",
                    subtaskId,
                    scope,
                    readerGroupName);
            enumContext.assignSplit(new PravegaSplit(readerGroupName, subtaskId), subtaskId);
        }
    }

    // deal with the data recovery, it will call readerGroup::initiateCheckpoint to get a checkpoint for recovery.
    // this call will be handled in another thread pool instead of the split enumerator thread.
    @Override
    public Checkpoint snapshotState(long chkPtID) throws Exception {
        ensureScheduledExecutorExists();

        final String checkpointName = createCheckpointName(chkPtID);

        LOG.info("Initiate Pravega checkpoint {}", checkpointName);
        final CompletableFuture<Checkpoint> checkpointResult =
                this.readerGroup.initiateCheckpoint(checkpointName, scheduledExecutorService);
        try {
            this.checkpoint = checkpointResult.get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.error("Pravega checkpoint met error.", e);
            throw e;
        }

        return checkpoint;
    }

    @Override
    public void close() throws IOException {
        Throwable ex = null;
        if (readerGroupManager != null) {
            try {
                LOG.info("Closing Pravega ReaderGroupManager");
                readerGroupManager.close();
            } catch (Throwable e) {
                if (e instanceof InterruptedException) {
                    LOG.warn("Interrupted while waiting for ReaderGroupManager to close, retrying ...");
                    readerGroupManager.close();
                } else {
                    ex = ExceptionUtils.firstOrSuppressed(e, ex);
                }
            }
        }

        if (readerGroup != null) {
            try {
                LOG.info("Closing Pravega ReaderGroup");
                readerGroup.close();
            } catch (Throwable e) {
                if (e instanceof InterruptedException) {
                    LOG.warn("Interrupted while waiting for ReaderGroup to close, retrying ...");
                    readerGroup.close();
                } else {
                    ex = ExceptionUtils.firstOrSuppressed(e, ex);
                }
            }
        }

        if (ex instanceof Exception) {
            throw new IOException(ex);
        }

        if (scheduledExecutorService != null ) {
            LOG.info("Closing Scheduled Executor.");
            scheduledExecutorService.shutdownNow();
            scheduledExecutorService = null;
        }
    }

    // This method is called when there is an attempt to locally recover from a failure to assign unassigned splits.
    // It throws an intentional exception here to trigger a global recovery since Pravega
    // doesn't have a mechanism to recover from partial failure. This will shut down the reader group,
    // recreate it and recover it from the latest checkpoint.
    @Override
    public void addSplitsBack(List<PravegaSplit> splits, int subtaskId) {
        if (!isRecovered) {
            isRecovered = true;
            throw new RuntimeException("triggering global failure");
        }
    }

    // ------------------------------------------------------------------------
    //  utils
    // ------------------------------------------------------------------------

    /**
     * Create the {@link ReaderGroup} for the current configuration.
     */
    protected ReaderGroup createReaderGroup() {
        LOG.info("Creating reader group {} with reader group config: {}", readerGroupName, readerGroupConfig);
        readerGroupManager.createReaderGroup(readerGroupName, readerGroupConfig);
        return readerGroupManager.getReaderGroup(readerGroupName);
    }

    /**
     * Create the {@link ReaderGroupManager} for the current configuration.
     *
     * @return An instance of {@link ReaderGroupManager}
     */
    protected ReaderGroupManager createReaderGroupManager() {
        LOG.info("Creating reader group manager in scope: {}.", scope);
        return ReaderGroupManager.withScope(scope, clientConfig);
    }

    private void ensureScheduledExecutorExists() {
        if (scheduledExecutorService == null) {
            LOG.info("Creating Scheduled Executor for enumerator");
            scheduledExecutorService = Executors.newScheduledThreadPool(DEFAULT_CHECKPOINT_THREAD_POOL_SIZE);
        }
    }

    static String createCheckpointName(long checkpointId) {
        return "PVG-CHK-" + checkpointId;
    }
}
