/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink.watermark;

import io.pravega.client.stream.TimeWindow;
import org.apache.flink.annotation.Public;
import org.apache.flink.streaming.api.functions.TimestampAssigner;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

@Public
public interface AssignerWithTimeWindows<T> extends TimestampAssigner<T> {

    // app must implement the timestamp extraction
    @Nullable
    Watermark getWatermark(TimeWindow window);
}
