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
import io.pravega.client.stream.*;
import io.pravega.connectors.flink.util.FlinkPravegaUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.*;

@Slf4j
public class PravegaSplitReader<T> implements SplitReader<EventRead<T>, PravegaSplit> {
    private EventStreamReader<T> pravegaReader;
    private PravegaSplit split;

    private final ClientConfig clientConfig;

    // The Pravega reader config.
    private final ReaderConfig readerConfig;

    // The scope name of the reader group.
    private final String scope;

    // The readergroup name to coordinate the parallel readers. This should be unique for a Flink job.
    private final String readerGroupName;

    // The supplied event deserializer.
    private final DeserializationSchema<T> deserializationSchema;

    // the timeout for reading events from Pravega
    private final Time eventReadTimeout;

    public PravegaSplitReader(
            ClientConfig clientConfig,
            String readerGroupScopeName,
            String readerGroupName,
            DeserializationSchema<T> deserializationSchema,
            ReaderConfig readerConfig,
            Time eventReadTimeout) {
        this.eventReadTimeout = eventReadTimeout;
        this.clientConfig = clientConfig;
        this.scope = readerGroupScopeName;
        this.readerGroupName = readerGroupName;
        this.deserializationSchema = deserializationSchema;
        this.readerConfig = readerConfig;
    }

    @Override
    public RecordsWithSplitIds<EventRead<T>> fetch() throws IOException {
        RecordsBySplits.Builder<EventRead<T>> records = new RecordsBySplits.Builder<>();
        EventRead<T> eventRead = null;
        do {
            try {
                eventRead = pravegaReader.readNextEvent(eventReadTimeout.toMilliseconds());
                log.info("read event: {} on reader {}", eventRead.getEvent(), split.getSubtaskId());
            } catch (TruncatedDataException e) {
                continue;
            }
            if (eventRead.getEvent() != null) {
                records.add(split.splitId(), eventRead);
            }
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

        // TODO: If we can know the subtaskID of the split reader as legacy source,
        // we can have a complete no-op split approach.
        pravegaReader = FlinkPravegaUtils.createPravegaReader(
                clientConfig,
                split.splitId(),
                scope,
                readerGroupName,
                deserializationSchema,
                readerConfig);

        log.info("Create reader for subtask: {}", split.getSubtaskId());
    }

    @Override
    public void wakeUp() {
        log.info("Call wakeup");
    }

    protected EventStreamReader<T> getPravegaReader() {
        return pravegaReader;
    }
}
