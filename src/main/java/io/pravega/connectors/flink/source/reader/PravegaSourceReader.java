/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink.source.reader;

import io.pravega.client.ClientConfig;
import io.pravega.client.stream.EventRead;
import io.pravega.connectors.flink.source.split.PravegaSplit;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.connector.source.ExternallyInducedSourceReader;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.InputStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * An Pravega implementation of {@link SourceReader}
 *
 * @param <T> The final element type to emit.
 */

@Slf4j
public class PravegaSourceReader<T>
        implements ExternallyInducedSourceReader<T, PravegaSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(PravegaSourceReader.class);

    /** A queue to buffer the elements fetched by the fetcher thread. */
    private final FutureCompletingBlockingQueue<RecordsWithSplitIds<EventRead<T>>> elementsQueue;

    /** The state of the splits. */
    private final Map<String, SplitContext<T, PravegaSplit>> splitStates;

    /** The record emitter to handle the records read by the SplitReaders. */
    protected final RecordEmitter<EventRead<T>, T, PravegaSplit> recordEmitter;

    /** The split fetcher manager to run split fetchers. */
    protected final SplitFetcherManager<EventRead<T>, PravegaSplit> splitFetcherManager;

    /** The configuration for the reader. */
    protected final SourceReaderOptions options;

    /** The raw configurations that may be used by subclasses. */
    protected final Configuration config;

    /** The context of this source reader. */
    protected SourceReaderContext context;

    /** The latest fetched batch of records-by-split from the split reader. */
    @Nullable
    private RecordsWithSplitIds<EventRead<T>> currentFetch;

    @Nullable private SplitContext<T, PravegaSplit> currentSplitContext;
    @Nullable private SourceOutput<T> currentSplitOutput;

    /** Indicating whether the SourceReader will be assigned more splits or not. */
    private boolean noMoreSplitsAssignment;

    public PravegaSourceReader(
            Supplier<PravegaSplitReader<T>> splitReaderSupplier,
            RecordEmitter<EventRead<T>, T, PravegaSplit> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        this.elementsQueue = new FutureCompletingBlockingQueue<>(
                config.getInteger(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY));
        this.splitFetcherManager = new PravegaFetcherManager<>(elementsQueue, splitReaderSupplier::get);
        this.recordEmitter = recordEmitter;
        this.splitStates = new HashMap<>();
        this.options = new SourceReaderOptions(config);
        this.config = config;
        this.context = context;
        this.noMoreSplitsAssignment = false;
    }

    @Override
    public void start() {}

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
        // make sure we have a fetch we are working on, or move to the next
        RecordsWithSplitIds<EventRead<T>> recordsWithSplitId = this.currentFetch;
        if (recordsWithSplitId == null) {
            recordsWithSplitId = getNextFetch(output);
            if (recordsWithSplitId == null) {
                return trace(finishedOrAvailableLater());
            }
        }

        // we need to loop here, because we may have to go across splits
        while (true) {
            // Process one record.
            final EventRead<T> record = recordsWithSplitId.nextRecordFromSplit();
            if (record != null) {
                if (record.isCheckpoint()) {
                    return trace(InputStatus.NOTHING_AVAILABLE);
                }
                // emit the record.
                recordEmitter.emitRecord(record, currentSplitOutput, currentSplitContext.state);
                LOG.trace("Emitted record: {}", record);

                // We always emit MORE_AVAILABLE here, even though we do not strictly know whether
                // more is available. If nothing more is available, the next invocation will find
                // this out and return the correct status.
                // That means we emit the occasional 'false positive' for availability, but this
                // saves us doing checks for every record. Ultimately, this is cheaper.
                return trace(InputStatus.MORE_AVAILABLE);
            } else if (!moveToNextSplit(recordsWithSplitId, output)) {
                // The fetch is done and we just discovered that and have not emitted anything, yet.
                // We need to move to the next fetch. As a shortcut, we call pollNext() here again,
                // rather than emitting nothing and waiting for the caller to call us again.
                return pollNext(output);
            }
            // else fall through the loop
        }
    }

    private InputStatus trace(InputStatus status) {
        LOG.trace("Source reader status: {}", status);
        return status;
    }

    @Nullable
    private RecordsWithSplitIds<EventRead<T>> getNextFetch(final ReaderOutput<T> output) {
        splitFetcherManager.checkErrors();

        LOG.trace("Getting next source data batch from queue");
        final RecordsWithSplitIds<EventRead<T>> recordsWithSplitId = elementsQueue.poll();
        if (recordsWithSplitId == null || !moveToNextSplit(recordsWithSplitId, output)) {
            // No element available, set to available later if needed.
            return null;
        }

        currentFetch = recordsWithSplitId;
        return recordsWithSplitId;
    }

    private void finishCurrentFetch(
            final RecordsWithSplitIds<EventRead<T>> fetch, final ReaderOutput<T> output) {
        currentFetch = null;
        currentSplitContext = null;
        currentSplitOutput = null;

        final Set<String> finishedSplits = fetch.finishedSplits();
        if (!finishedSplits.isEmpty()) {
            LOG.info("Finished reading split(s) {}", finishedSplits);
            Map<String, PravegaSplit> stateOfFinishedSplits = new HashMap<>();
            for (String finishedSplitId : finishedSplits) {
                stateOfFinishedSplits.put(
                        finishedSplitId, splitStates.remove(finishedSplitId).state);
                output.releaseOutputForSplit(finishedSplitId);
            }
            onSplitFinished(stateOfFinishedSplits);
        }

        fetch.recycle();
    }

    private boolean moveToNextSplit(
            RecordsWithSplitIds<EventRead<T>> recordsWithSplitIds, ReaderOutput<T> output) {
        final String nextSplitId = recordsWithSplitIds.nextSplit();
        if (nextSplitId == null) {
            LOG.trace("Current fetch is finished.");
            finishCurrentFetch(recordsWithSplitIds, output);
            return false;
        }

        currentSplitContext = splitStates.get(nextSplitId);
        checkState(currentSplitContext != null, "Have records for a split that was not registered");
        currentSplitOutput = currentSplitContext.getOrCreateSplitOutput(output);
        LOG.trace("Emitting records from fetch for split {}", nextSplitId);
        return true;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return currentFetch != null
                ? FutureCompletingBlockingQueue.AVAILABLE
                : elementsQueue.getAvailabilityFuture();
    }

    @Override
    public List<PravegaSplit> snapshotState(long checkpointId) {
        List<PravegaSplit> splits = new ArrayList<>();
        splitStates.forEach((id, context) -> splits.add(toSplitType(id, context.state)));
        return splits;
    }

    @Override
    public void addSplits(List<PravegaSplit> splits) {
        LOG.info("Adding split(s) to reader: {}", splits);
        // Initialize the state for each split.
        splits.forEach(
                s ->
                        splitStates.put(
                                s.splitId(), new SplitContext<>(s.splitId(), initializedState(s))));
        // Hand over the splits to the split fetcher to start fetch.
        splitFetcherManager.addSplits(splits);
    }

    @Override
    public void notifyNoMoreSplits() {
        LOG.info("Reader received NoMoreSplits event.");
        noMoreSplitsAssignment = true;
        elementsQueue.notifyAvailable();
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        LOG.info("Received unhandled source event: {}", sourceEvent);
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing Source Reader.");
        splitFetcherManager.close(options.sourceReaderCloseTimeout);
    }

    /**
     * Gets the number of splits the reads has currently assigned.
     *
     * <p>These are the splits that have been added via {@link #addSplits(List)} and have not yet
     * been finished by returning them from the {@link SplitReader#fetch()} as part of {@link
     * RecordsWithSplitIds#finishedSplits()}.
     */
    public int getNumberOfCurrentlyAssignedSplits() {
        return splitStates.size();
    }

    // -------------------- Abstract method to allow different implementations ------------------
    /** Handles the finished splits to clean the state if needed. */
    protected void onSplitFinished(Map<String, PravegaSplit> finishedSplitIds) {}

    /**
     * When new splits are added to the reader. The initialize the state of the new splits.
     *
     * @param split a newly added split.
     */
    protected PravegaSplit initializedState(PravegaSplit split) {
        return split;
    }

    /**
     * Convert a mutable SplitStateT to immutable SplitT.
     *
     * @param splitState splitState.
     * @return an immutable Split state.
     */
    protected PravegaSplit toSplitType(String splitId, PravegaSplit splitState) {
        return splitState;
    }

    // ------------------ private helper methods ---------------------

    private InputStatus finishedOrAvailableLater() {
        final boolean allFetchersHaveShutdown = splitFetcherManager.maybeShutdownFinishedFetchers();
        if (!(noMoreSplitsAssignment && allFetchersHaveShutdown)) {
            return InputStatus.NOTHING_AVAILABLE;
        }
        if (elementsQueue.isEmpty()) {
            // We may reach here because of exceptional split fetcher, check it.
            splitFetcherManager.checkErrors();
            return InputStatus.END_OF_INPUT;
        } else {
            // We can reach this case if we just processed all data from the queue and finished a
            // split,
            // and concurrently the fetcher finished another split, whose data is then in the queue.
            return InputStatus.MORE_AVAILABLE;
        }
    }

    // ------------------ private helper classes ---------------------

    private static final class SplitContext<T, SplitStateT> {

        final String splitId;
        final SplitStateT state;
        SourceOutput<T> sourceOutput;

        private SplitContext(String splitId, SplitStateT state) {
            this.state = state;
            this.splitId = splitId;
        }

        SourceOutput<T> getOrCreateSplitOutput(ReaderOutput<T> mainOutput) {
            if (sourceOutput == null) {
                sourceOutput = mainOutput.createOutputForSplit(splitId);
            }
            return sourceOutput;
        }
    }

    @Override
    public Optional<Long> shouldTriggerCheckpoint() {
        return Optional.empty();
    }
}
