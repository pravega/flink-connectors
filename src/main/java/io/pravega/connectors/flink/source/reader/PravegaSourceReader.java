/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink.source.reader;

import io.pravega.client.stream.EventRead;
import io.pravega.connectors.flink.source.split.PravegaSplit;
import org.apache.flink.api.connector.source.ExternallyInducedSourceReader;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.InputStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * An Pravega implementation of {@link SourceReader}
 *
 * @param <T> The final element type to emit.
 */
public class PravegaSourceReader<T> extends SourceReaderBase<EventRead<T>, T, PravegaSplit, PravegaSplit>
        implements ExternallyInducedSourceReader<T, PravegaSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(PravegaSourceReader.class);

    private Optional<Long> checkpointId;

    private PravegaSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<EventRead<T>>> elementsQueue,
            Supplier<PravegaSplitReader<T>> splitReaderSupplier,
            RecordEmitter<EventRead<T>, T, PravegaSplit> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(elementsQueue, new PravegaFetcherManager<>(elementsQueue, splitReaderSupplier::get), recordEmitter, config, context);
        checkpointId = Optional.empty();
    }

    public PravegaSourceReader(
            Supplier<PravegaSplitReader<T>> splitReaderSupplier,
            RecordEmitter<EventRead<T>, T, PravegaSplit> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        this(new FutureCompletingBlockingQueue<>(config.getInteger(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY)),
                splitReaderSupplier,
                recordEmitter,
                config,
                context);
    }

    @Override
    public InputStatus pollNext(ReaderOutput output) throws Exception {
        InputStatus inputStatus = super.pollNext(output);
        checkpointId = ((PravegaRecordEmitter) recordEmitter).getAndResetCheckpointId();

        // When the record is a checkpoint record, set the inputStatus to NOTHING_AVAILABLE
        if (checkpointId.isPresent()) {
            inputStatus = InputStatus.NOTHING_AVAILABLE;
            LOG.trace("Source reader status: {}", inputStatus);
        }
        return inputStatus;
    }

    @Override
    public List<PravegaSplit> snapshotState(long checkpointId) {
        // Pravega doesn't support partial failover recover so just return null here for Source Reader
        return null;
    }

    @Override
    public Optional<Long> shouldTriggerCheckpoint() {
        return checkpointId;
    }

    @Override
    protected void onSplitFinished(Map finishedSplitIds) {

    }

    @Override
    protected PravegaSplit initializedState(PravegaSplit split) {
        return split;
    }

    @Override
    protected PravegaSplit toSplitType(String splitId, PravegaSplit splitState) {
        return null;
    }
}
