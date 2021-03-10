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

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.connectors.flink.source.PravegaSourceOptions;
import io.pravega.connectors.flink.source.split.PravegaSplit;
import io.pravega.connectors.flink.util.FlinkPravegaUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

@Slf4j
public class PravegaSplitReader<T>
        implements SplitReader<EventRead<T>, PravegaSplit> {
    private EventStreamReader<T> pravegaReader;

    public PravegaSplit split;

    private final Configuration options;

    private final int subtaskId;

    public PravegaSplitReader(
            EventStreamClientFactory eventStreamClientFactory,
            String readerGroupName,
            DeserializationSchema<T> deserializationSchema,
            int subtaskId) {
        this.subtaskId = subtaskId;
        this.options = new Configuration();
        this.pravegaReader = FlinkPravegaUtils.createPravegaReader(
                PravegaSplit.splitId(subtaskId),
                readerGroupName,
                deserializationSchema,
                ReaderConfig.builder().build(),
                eventStreamClientFactory);
    }

    @Override
    public RecordsWithSplitIds<EventRead<T>> fetch() throws IOException {
        RecordsBySplits.Builder<EventRead<T>> records = new RecordsBySplits.Builder<>();
        EventRead<T> eventRead = null;
        do {
            try {
                eventRead = pravegaReader.readNextEvent(
                        options.getLong(PravegaSourceOptions.READER_TIMEOUT_MS));
                log.info("read event: {} on reader {}", eventRead.getEvent(), subtaskId);
            } catch (TruncatedDataException e) {
                continue;
            }
            records.add(split, eventRead);
        } while (eventRead == null || eventRead.getEvent() == null);
        return records.build();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<PravegaSplit> splitsChange) {
        if (splitsChange instanceof SplitsAddition) {
            // One reader for one split
            Preconditions.checkArgument(splitsChange.splits().size() == 1);
            this.split = splitsChange.splits().get(0);
        }
    }

    @Override
    public void wakeUp() {
        log.info("Call wakeup");
    }

    @Override
    public void close() {
        pravegaReader.close();
    }
}
