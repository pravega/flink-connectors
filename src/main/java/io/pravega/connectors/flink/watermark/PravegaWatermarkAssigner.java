/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink.watermark;

import io.pravega.client.stream.EventStreamReader;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public abstract class PravegaWatermarkAssigner<T> implements AssignerWithPeriodicWatermarks<T> {
    private EventStreamReader<T> pravegaReader;

    public PravegaWatermarkAssigner(EventStreamReader pravegaReader) {
        this.pravegaReader = pravegaReader;
    }

    // app must implement the timestamp extraction
    public abstract long extractTimestamp(T element);

    @Override
    public long extractTimestamp(T element, long previousElementTimestamp) {
        return this.extractTimestamp(element);
    }

    // built-in watermark implementation which emits the lower bound - 1
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(pravegaReader.getCurrentTimeWindow().getLowerTimeBound() - 1L);
    }
}

