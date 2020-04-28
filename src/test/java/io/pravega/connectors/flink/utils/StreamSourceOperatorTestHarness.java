/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink.utils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.checkpoint.ExternallyInducedSource;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.util.FlinkException;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

/**
 * A test harness for {@link StreamSource StreamSource} operators and {@link SourceFunction SourceFunctions}.
 *
 * @param <T>  The output type of the source.
 * @param <F>  The type of the source function.
 */
public class StreamSourceOperatorTestHarness<T, F extends SourceFunction<T>> extends AbstractStreamOperatorTestHarness<T> implements AutoCloseable {

    private final StreamSource<T, F> sourceOperator;

    private final ConcurrentLinkedQueue<Long> triggeredCheckpoints;

    private final OperatorChain<T, ?> operatorChain;

    public StreamSourceOperatorTestHarness(F sourceFunction, int maxParallelism, int parallelism, int subtaskIndex) throws Exception {
        this(new StreamSource<>(sourceFunction), maxParallelism, parallelism, subtaskIndex);
    }

    public StreamSourceOperatorTestHarness(StreamSource<T, F> operator, int maxParallelism, int parallelism, int subtaskIndex) throws Exception {
        super(operator, maxParallelism, parallelism, subtaskIndex);
        this.sourceOperator = operator;
        this.triggeredCheckpoints = new ConcurrentLinkedQueue<>();
        this.operatorChain = new OperatorChain<>(this.mockTask, StreamTask.createRecordWriterDelegate(this.config, this.getEnvironment()));
    }

    @Override
    public void setup(TypeSerializer<T> outputSerializer) {
        super.setup(outputSerializer);
        if (sourceOperator.getUserFunction() instanceof ExternallyInducedSource) {
            ExternallyInducedSource externallyInducedSource = (ExternallyInducedSource) sourceOperator.getUserFunction();
            externallyInducedSource.setCheckpointTrigger(this::triggerCheckpoint);
        }
    }

    /**
     * Runs the source operator synchronously.
     * @throws Exception if execution fails.
     */
    public void run() throws Exception {
        sourceOperator.run(this.getCheckpointLock(), this.mockTask.getStreamStatusMaintainer(), operatorChain);
    }

    /**
     * Runs the source operator asychronously with cancellation support.
     *
     * @param executor the executor on which to invoke the {@code run} method.
     * @return a future that completes when the {@code run} method of the {@link StreamSource} completes.
     */
    public CompletableFuture<Void> runAsync(Executor executor) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        promise.whenComplete((v, ex) -> {
            if (ex instanceof CancellationException) {
                sourceOperator.cancel();
            }
        });
        executor.execute(() -> {
            try {
                run();
                promise.complete(null);
            } catch (Throwable ex) {
                promise.completeExceptionally(ex);
            }
        });
        return promise;
    }

    /**
     * Sends a cancellation notice to the source operator.
     */
    public void cancel() {
        sourceOperator.cancel();
    }

    /**
     * Invoked when an {@link ExternallyInducedSource externally-induced source} triggers a checkpoint.
     *
     * The default behavior is to record the checkpoint ID for later.
     *
     * @param checkpointId the checkpoint ID
     * @throws FlinkException if the checkpoint cannot be triggered.
     *
     */
    protected void triggerCheckpoint(long checkpointId) throws FlinkException {
        triggeredCheckpoints.add(checkpointId);
    }

    /**
     * Gets the triggered checkpoints.
     */
    public ConcurrentLinkedQueue<Long> getTriggeredCheckpoints() {
        return triggeredCheckpoints;
    }

    /**
     * Gets handle to the MetricGroup.
     */
    public MetricGroup getMetricGroup() {
        return sourceOperator.getMetricGroup();
    }
}
