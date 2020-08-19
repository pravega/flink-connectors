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

import io.pravega.client.stream.EventRead;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.base.source.reader.synchronization.FutureNotifier;

import java.util.List;
import java.util.function.Supplier;

public class PravegaFetcherManager<T>
        extends SplitFetcherManager<EventRead<T>, PravegaSplit> {

    public PravegaFetcherManager(
            FutureNotifier futureNotifier,
            FutureCompletingBlockingQueue<RecordsWithSplitIds<EventRead<T>>> elementsQueue,
            Supplier<SplitReader<EventRead<T>, PravegaSplit>> splitReaderSupplier) {
        super(futureNotifier, elementsQueue, splitReaderSupplier);
    }

    @Override
    public void addSplits(List<PravegaSplit> splitsToAdd) {
        for (PravegaSplit split : splitsToAdd) {
            SplitFetcher<EventRead<T>, PravegaSplit> fetcher = createSplitFetcher();
            // Add the splits to the fetchers.
            fetcher.addSplits(splitsToAdd);
            startFetcher(fetcher);
        }
    }

    @Override
    public synchronized void close(long timeoutMs) throws Exception {
        fetchers.forEach( (k ,v) -> v.shutdown());
        super.close(timeoutMs);
    }
}
