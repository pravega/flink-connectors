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

import io.pravega.client.stream.EventRead;
import io.pravega.connectors.flink.source.split.PravegaSplit;
import io.pravega.connectors.flink.util.FlinkPravegaUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** The {@link RecordEmitter} implementation for {@link PravegaSourceReader}. */
public class PravegaRecordEmitter<T> implements RecordEmitter<EventRead<ByteBuffer>, T, PravegaSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(PravegaRecordEmitter.class);

    // a simple event collector to collect event from deserialization
    private final SimpleCollector<T> collector;

    // supplied event deserializer.
    private final DeserializationSchema<T> deserializationSchema;

    // checkpoint ID of the latest record if record is a checkpoint record
    private Optional<Long> checkpointId;

    /**
     * Creates a new Pravega Record Emitter instance.
     * PravegaRecordEmitter turns {@link EventRead} into {@link T}.
     *
     * @param deserializationSchema   The implementation to deserialize events from Pravega streams.
     */
    public PravegaRecordEmitter(DeserializationSchema<T> deserializationSchema) {
        checkpointId = Optional.empty();
        this.collector = new SimpleCollector<>();
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void emitRecord(EventRead<ByteBuffer> record, SourceOutput<T> output, PravegaSplit state) throws Exception {
        // If the record to emit is a checkpoint, record the checkpoint ID for later
        if (record.isCheckpoint()) {
            String checkpointName = record.getCheckpointName();
            checkpointId = Optional.of(getCheckpointId(checkpointName));
            LOG.info("read checkpoint {} on reader {}", checkpointName, state.getSubtaskId());
        } else if (record.getEvent() != null) {
            deserializationSchema.deserialize(FlinkPravegaUtils.byteBufferToArray(record.getEvent()), collector);
            T event = collector.getRecords().get(collector.getRecords().size() - 1);
            output.collect(event);
        }
    }

    /**
     * Invoked right after the Source Reader called {@code emitRecord} and the record is a checkpoint record.
     * The behavior is to return the checkpoint ID after reset it.
     *
     * @return checkpointId
     */
    public Optional<Long> getAndResetCheckpointId() {
        Optional<Long> chkPt = checkpointId;
        checkpointId = Optional.empty();
        return chkPt;
    }

    private static Long getCheckpointId(String checkpointName) {
        return Long.valueOf(checkpointName.substring(8));
    }

    private static class SimpleCollector<T> implements Collector<T> {
        private final List<T> records = new ArrayList<>();

        @Override
        public void collect(T record) {
            records.add(record);
        }

        @Override
        public void close() {}

        private List<T> getRecords() {
            return records;
        }

        private void reset() {
            records.clear();
        }
    }
}
