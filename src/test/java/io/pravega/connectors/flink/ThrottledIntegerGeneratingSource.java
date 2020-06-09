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

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializableObject;

import java.util.Collections;
import java.util.List;

/**
 * A Flink source that generates integers, but slows down until the first checkpoint has been completed.
 */
@Slf4j
public class ThrottledIntegerGeneratingSource
        extends RichParallelSourceFunction<Integer>
        implements ListCheckpointed<Integer>, CheckpointListener {

    /** Blocker when the generator needs to wait for the checkpoint to happen.
     * Eager initialization means it must be serializable (pick any serializable type) */
    private final Object blocker = new SerializableObject();

    /** The total number of events to generate */
    private final int numEventsTotal;

    private final int latestPosForCheckpoint;

    /** The current position in the sequence of numbers */
    private int currentPosition = -1;

    private long lastCheckpointTriggered;

    private long lastCheckpointConfirmed;

    /** Flag to cancel the source. Must be volatile, because modified asynchronously */
    private volatile boolean running = true;

    private int eventsPerWatermark = 0;

    public ThrottledIntegerGeneratingSource(final int numEventsTotal) {
        this(numEventsTotal, numEventsTotal / 2);
    }

    public ThrottledIntegerGeneratingSource(final int numEventsTotal, final int latestPosForCheckpoint) {
        Preconditions.checkArgument(numEventsTotal > 0);
        Preconditions.checkArgument(latestPosForCheckpoint >= 0 && latestPosForCheckpoint < numEventsTotal);

        this.numEventsTotal = numEventsTotal;
        this.latestPosForCheckpoint = latestPosForCheckpoint;
    }

    /**
     * Tells the source to emit a watermark every {@link #eventsPerWatermark} events.
     *
     * @param eventsPerWatermark the number of events between two watermarks
     *
     * @return the configured {@link ThrottledIntegerGeneratingSource}
     */
    public ThrottledIntegerGeneratingSource withWatermarks(int eventsPerWatermark) {
        this.eventsPerWatermark = eventsPerWatermark;
        return this;
    }

    // ------------------------------------------------------------------------
    //  source
    // ------------------------------------------------------------------------

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {

        // each source subtask emits only the numbers where (num % parallelism == subtask_index)
        final int stepSize = getRuntimeContext().getNumberOfParallelSubtasks();
        int current = this.currentPosition >= 0 ? this.currentPosition : getRuntimeContext().getIndexOfThisSubtask();

        if (eventsPerWatermark > 0) {
            ctx.emitWatermark(new Watermark(current));
        }

        log.info("Running");
        while (this.running && current < this.numEventsTotal) {

            // throttle if no checkpoint happened so far
            if (this.lastCheckpointConfirmed < 1) {
                if (current < this.latestPosForCheckpoint) {
                    Thread.sleep(1);
                } else {
                    synchronized (blocker) {
                        while (this.running && this.lastCheckpointConfirmed < 1) {
                            blocker.wait();
                        }
                    }
                }
            }

            // emit the next element
            current += stepSize;
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(current);
                this.currentPosition = current;

                if (eventsPerWatermark > 0 && current % eventsPerWatermark == 0) {
                    ctx.emitWatermark(new Watermark(current));
                }
            }
        }

        // after we are done, we need to wait for two more checkpoint to complete
        // before finishing the program - that is to be on the safe side that
        // the sink also got the "commit" notification for all relevant checkpoints
        // and committed the data to pravega

        // note: this indicates that to handle finite jobs with 2PC outputs more
        // easily, we need a primitive like "finish-with-checkpoint" in Flink

        // FLIP-34 will address this shortcomings which will take care of completing a savepoint/checkpoint
        // when the task is finishing successfully ensuring the end-to-end exactly once guarantee for the sink

        final long lastCheckpoint;
        synchronized (ctx.getCheckpointLock()) {
            lastCheckpoint = this.lastCheckpointTriggered;
        }

        synchronized (this.blocker) {
            while (this.lastCheckpointConfirmed <= lastCheckpoint + 4) {
                this.blocker.wait();
            }
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }

    // ------------------------------------------------------------------------
    //  snapshots
    // ------------------------------------------------------------------------

    @Override
    public List<Integer> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
        this.lastCheckpointTriggered = checkpointId;
        return Collections.singletonList(this.currentPosition);
    }

    @Override
    public void restoreState(List<Integer> state) throws Exception {
        this.currentPosition = state.get(0);

        // at least one checkpoint must have happened so fat
        this.lastCheckpointTriggered = 1L;
        this.lastCheckpointConfirmed = 1L;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        synchronized (blocker) {
            this.lastCheckpointConfirmed = checkpointId;
            blocker.notifyAll();
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {

    }
}
