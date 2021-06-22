/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Queue;

public class PravegaCollector<T> implements Collector<T>, Serializable {
    private static final long serialVersionUID = 1L;

    private final DeserializationSchema<T> deserializationSchema;

    private final Queue<T> records = new ArrayDeque<>();

    private boolean endOfStreamSignalled = false;

    public PravegaCollector(DeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void collect(T record) {
        // do not emit subsequent elements if the end of the stream reached
        if (endOfStreamSignalled || deserializationSchema.isEndOfStream(record)) {
            endOfStreamSignalled = true;
            return;
        }
        records.add(record);
    }

    public Queue<T> getRecords() {
        return records;
    }

    public boolean isEndOfStreamSignalled() {
        return endOfStreamSignalled;
    }

    @Override
    public void close() {}
}
