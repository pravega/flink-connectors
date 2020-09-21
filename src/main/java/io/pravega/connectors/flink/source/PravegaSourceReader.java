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
import io.pravega.client.stream.EventRead;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import java.util.Collection;
import java.util.function.Supplier;

/**
 * An Pravega implementation of {@link SourceReader}
 *
 * @param <T> The final element type to emit.
 */

@Slf4j
public class PravegaSourceReader<T>
        extends SourceReaderBase<EventRead<T>, T, PravegaSplit, PravegaSplit> {

    public PravegaSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<EventRead<T>>> elementsQueue,
            ClientConfig clientConfig,
            String scope,
            String readerGroupName,
            DeserializationSchema<T> deserializationSchema,
            Time eventReadTimeout,
            RecordEmitter<EventRead<T>, T, PravegaSplit> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(
                elementsQueue,
                new PravegaFetcherManager<>(elementsQueue,
                        clientConfig,
                        scope,
                        readerGroupName,
                        deserializationSchema,
                        eventReadTimeout),
                recordEmitter,
                config,
                context);
    }

    @Override
    public void start() {
        log.info("Source Reader start");
    }

    @Override
    protected void onSplitFinished(Collection<String> finishedSplitIds) {
    }

    @Override
    protected PravegaSplit initializedState(PravegaSplit split) {
        return split;
    }

    @Override
    protected PravegaSplit toSplitType(String splitId, PravegaSplit splitState) {
        return splitState;
    }
}
