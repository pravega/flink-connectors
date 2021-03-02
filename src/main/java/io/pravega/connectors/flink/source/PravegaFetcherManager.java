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
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

@Slf4j
public class PravegaFetcherManager<T>
        extends SingleThreadFetcherManager<EventRead<T>, PravegaSplit> {

    private static EventStreamClientFactory eventStreamClientFactory;
    private static PravegaSplitReader splitReader = null;

    public PravegaFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<EventRead<T>>> elementsQueue,
            ClientConfig clientConfig,
            String scope,
            String readerGroupName,
            DeserializationSchema<T> deserializationSchema,
            Time eventReadTimeout) {
        super(elementsQueue, () -> {
            if (eventStreamClientFactory == null) {
                eventStreamClientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
            }

            splitReader = new PravegaSplitReader<>(
                    eventStreamClientFactory,
                    readerGroupName,
                    deserializationSchema,
                    ReaderConfig.builder().build(),
                    eventReadTimeout);

            return splitReader;
        });
    }

    @Override
    public synchronized void close(long timeoutMs) throws Exception {
        splitReader.close();
        log.info("Close reader {} ...", splitReader.split.getSubtaskId());
        splitReader = null;
        super.close(timeoutMs);
    }
}
