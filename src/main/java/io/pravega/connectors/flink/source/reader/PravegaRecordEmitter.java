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
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

@Slf4j
public class PravegaRecordEmitter<T> implements RecordEmitter<EventRead<T>, T, PravegaSplit> {
    @Override
    public void emitRecord(EventRead<T> record, SourceOutput<T> output, PravegaSplit state) throws Exception {
        // The value is the first element.
        output.collect(record.getEvent());
    }
}
