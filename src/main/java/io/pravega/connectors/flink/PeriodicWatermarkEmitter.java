/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink;

import io.pravega.client.stream.EventStreamReader;
import io.pravega.connectors.flink.watermark.AssignerWithTimeWindows;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Preconditions;

class PeriodicWatermarkEmitter implements ProcessingTimeCallback {

    private EventStreamReader<?> pravegaReader;
    private AssignerWithTimeWindows<?> assignerWithTimeWindows;
    private final SourceContext<?> ctx;
    private final ProcessingTimeService timerService;
    private final long interval;
    private long lastWatermarkTimestamp;

    protected PeriodicWatermarkEmitter(
            EventStreamReader<?> pravegaReader, AssignerWithTimeWindows<?> assignerWithTimeWindows,
            SourceContext<?> ctx, ProcessingTimeService timerService, long autoWatermarkInterval) {
        this.pravegaReader = Preconditions.checkNotNull(pravegaReader);
        this.assignerWithTimeWindows = Preconditions.checkNotNull(assignerWithTimeWindows);
        this.ctx = Preconditions.checkNotNull(ctx);
        this.timerService = Preconditions.checkNotNull(timerService);
        this.interval = autoWatermarkInterval;
        this.lastWatermarkTimestamp = Long.MIN_VALUE;
    }

    protected void start() {
        timerService.registerTimer(timerService.getCurrentProcessingTime() + interval, this);
    }

    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        Watermark watermark = assignerWithTimeWindows.getWatermark(pravegaReader.getCurrentTimeWindow());

        if (watermark.getTimestamp() > lastWatermarkTimestamp) {
            lastWatermarkTimestamp = watermark.getTimestamp();
            ctx.emitWatermark(watermark);
        }

        // schedule the next watermark
        timerService.registerTimer(timerService.getCurrentProcessingTime() + interval, this);
    }
}
