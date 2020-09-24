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
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.ReaderConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

@Slf4j
public class PravegaFetcherManager<T>
        extends SplitFetcherManager<EventRead<T>, PravegaSplit> {

    private static EventStreamClientFactory eventStreamClientFactory;
    private static List<PravegaSplitReader> splitReaders = new ArrayList<>();

    public PravegaFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<EventRead<T>>> elementsQueue,
            ClientConfig clientConfig,
            String scope,
            String readerGroupName,
            DeserializationSchema<T> deserializationSchema,
            Time eventReadTimeout) {
        super(elementsQueue, () -> {
            if (eventStreamClientFactory != null) {
                eventStreamClientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
            }

            PravegaSplitReader<T> splitReader = new PravegaSplitReader<>(
                    eventStreamClientFactory,
                    readerGroupName,
                    deserializationSchema,
                    ReaderConfig.builder().build(),
                    eventReadTimeout);

            splitReaders.add(splitReader);
            return splitReader;
        });
    }

    @Override
    public void addSplits(List<PravegaSplit> splitsToAdd) {
        log.info("splitReaders: {}", splitReaders.size());
        SplitFetcher<EventRead<T>, PravegaSplit> fetcher = fetchers.get(0);
        if (fetcher == null) {
            fetcher = createSplitFetcher();
            // Add the splits to the fetchers.
            fetcher.addSplits(splitsToAdd);
            startFetcher(fetcher);
        } else {
            fetcher.addSplits(splitsToAdd);
        }
    }

    @Override
    public synchronized void close(long timeoutMs) throws Exception {
        splitReaders.forEach( (r) -> r.getPravegaReader().close());
        log.info("Close readers");
        splitReaders.clear();
        super.close(timeoutMs);
    }
}
