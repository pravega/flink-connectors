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
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.base.source.event.NoMoreSplitsEvent;

import java.io.IOException;
import java.util.*;

@Slf4j
public class PravegaSplitEnumerator implements SplitEnumerator<PravegaSplit, List<PravegaSplit>> {
    private final SplitEnumeratorContext<PravegaSplit> enumContext;
    private ReaderGroup readerGroup;
    private ReaderGroupManager readerGroupManager;
    private ClientConfig clientConfig;
    private ReaderGroupConfig readerGroupConfig;
    private String scope;
    private String readerGroupName;
    private List<PravegaSplit> splits;

    public PravegaSplitEnumerator(
            SplitEnumeratorContext<PravegaSplit> context,
            String scope,
            String readerGroupName,
            ClientConfig clientConfig,
            ReaderGroupConfig readerGroupConfig) {
        this.enumContext = context;
        this.scope = scope;
        this.readerGroupName = readerGroupName;
        this.clientConfig = clientConfig;
        this.readerGroupConfig = readerGroupConfig;
        this.splits = new ArrayList<>();
        for (int i = 0; i < enumContext.currentParallelism(); i++) {
            splits.add(new PravegaSplit(readerGroupName, i));
        }
    }

    @Override
    public void start() {

        log.info("Creating reader group: {}/{}.", this.scope, this.readerGroupName);

        if (this.readerGroupManager == null) {
            this.readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig);
        }

        if (this.readerGroup == null) {
            readerGroupManager.createReaderGroup(this.readerGroupName, readerGroupConfig);
            this.readerGroup = readerGroupManager.getReaderGroup(this.readerGroupName);
        }
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
    }

    @Override
    public void addReader(int subtaskId) {
        if (enumContext.registeredReaders().size() == enumContext.currentParallelism() && splits.size() > 0) {
            int numReaders = enumContext.registeredReaders().size();
            Map<Integer, List<PravegaSplit>> assignment = new HashMap<>();
            for (int i = 0; i < numReaders; i++) {
                assignment.put(i, Collections.singletonList(splits.get(i)));
            }
            enumContext.assignSplits(new SplitsAssignment<>(assignment));
            splits.clear();
            for (int i = 0; i < numReaders; i++) {
                enumContext.sendEventToSourceReader(i, new NoMoreSplitsEvent());
            }
        }
    }

    @Override
    public List<PravegaSplit> snapshotState() throws Exception {
        return splits;
    }

    @Override
    public void close() throws IOException {
        readerGroup.close();
        readerGroupManager.deleteReaderGroup(readerGroupName);
        readerGroupManager.close();
    }

    @Override
    public void addSplitsBack(List<PravegaSplit> splits, int subtaskId) {
        this.splits.addAll(splits);
    }
}
