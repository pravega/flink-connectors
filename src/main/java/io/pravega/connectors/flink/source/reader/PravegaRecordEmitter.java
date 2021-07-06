/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink.source.reader;

import io.pravega.client.stream.EventRead;
import io.pravega.connectors.flink.source.split.PravegaSplit;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/** The {@link RecordEmitter} implementation for {@link PravegaSourceReader}. */
public class PravegaRecordEmitter<T> implements RecordEmitter<EventRead<T>, T, PravegaSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(PravegaRecordEmitter.class);

    // checkpoint ID of the latest record if record is a checkpoint record
    private Optional<Long> checkpointId;

    public PravegaRecordEmitter() {
        checkpointId = Optional.empty();
    }

    @Override
    public void emitRecord(EventRead<T> record, SourceOutput<T> output, PravegaSplit state) throws Exception {
        // If the record to emit is a checkpoint, record the checkpoint ID for later
        if (record.isCheckpoint()) {
            String checkpointName = record.getCheckpointName();
            checkpointId = Optional.of(getCheckpointId(checkpointName));
            LOG.info("read checkpoint {} on reader {}", checkpointName, state.getSubtaskId());
        } else if (record.getEvent() != null) {
            output.collect(record.getEvent());
        }
    }

    /**
     * Invoked right after the Source Reader called {@code emitRecord} and the record is a checkpoint record.
     * The behavior is to return the checkpoint ID after reset it.
     */
    public Optional<Long> getAndResetCheckpointId() {
        Optional<Long> chkPt = checkpointId;
        checkpointId = Optional.empty();
        return chkPt;
    }

    private static Long getCheckpointId(String checkpointName) {
        return Long.valueOf(checkpointName.substring(8));
    }
}
