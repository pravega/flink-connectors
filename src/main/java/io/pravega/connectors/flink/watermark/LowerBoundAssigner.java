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

import io.pravega.client.stream.TimeWindow;
import org.apache.flink.streaming.api.watermark.Watermark;

public abstract class LowerBoundAssigner<T> implements AssignerWithTimeWindows<T> {
    @Override
    public abstract long extractTimestamp(T element, long previousElementTimestamp);

    // built-in watermark implementation which emits the lower bound - 1
    @Override
    public Watermark getWatermark(TimeWindow timeWindow) {
        return new Watermark(timeWindow.getLowerTimeBound() - 1L);
    }
}

