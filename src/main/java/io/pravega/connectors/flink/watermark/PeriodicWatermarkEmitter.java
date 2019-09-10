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

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Preconditions;

public class PeriodicWatermarkEmitter implements ProcessingTimeCallback {

    private PravegaWatermarkAssigner<?> pravegaWatermarkAssigner;
    private final SourceContext<?> ctx;
    private final ProcessingTimeService timerService;
    private final long interval;
    private long lastWatermarkTimestamp;

    public PeriodicWatermarkEmitter(
            PravegaWatermarkAssigner<?> pravegaWatermarkAssigner,
            SourceContext<?> ctx,
            ProcessingTimeService timerService,
            long autoWatermarkInterval) {
        this.pravegaWatermarkAssigner = Preconditions.checkNotNull(pravegaWatermarkAssigner);
        this.ctx = Preconditions.checkNotNull(ctx);
        this.timerService = Preconditions.checkNotNull(timerService);
        this.interval = autoWatermarkInterval;
        this.lastWatermarkTimestamp = Long.MIN_VALUE;
    }

    public void start() {
        timerService.registerTimer(timerService.getCurrentProcessingTime() + interval, this);
    }

    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        Watermark watermark = pravegaWatermarkAssigner.getCurrentWatermark();

        if (watermark.getTimestamp() > lastWatermarkTimestamp) {
            lastWatermarkTimestamp = watermark.getTimestamp();
            ctx.emitWatermark(watermark);
        }

        // schedule the next watermark
        timerService.registerTimer(timerService.getCurrentProcessingTime() + interval, this);
    }
}
