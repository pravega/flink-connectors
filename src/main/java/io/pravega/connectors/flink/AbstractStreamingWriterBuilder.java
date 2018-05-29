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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.Preconditions;

/**
 * An abstract streaming writer builder.
 *
 * The builder is abstracted to act as the base for both the {@link FlinkPravegaWriter} and {@link FlinkPravegaTableSink} builders.
 *
 * @param <T> the element type.
 * @param <B> the builder type.
 */
public abstract class AbstractStreamingWriterBuilder<T, B extends AbstractStreamingWriterBuilder> extends AbstractWriterBuilder<B> {

    // the numbers below are picked somewhat arbitrarily at this point
    private static final long DEFAULT_TXN_TIMEOUT_MILLIS = 30000; // 30 seconds
    private static final long DEFAULT_TX_SCALE_GRACE_MILLIS = 30000; // 30 seconds

    protected PravegaWriterMode writerMode;
    protected Time txnTimeout;
    protected Time txnGracePeriod;

    protected AbstractStreamingWriterBuilder() {
        writerMode = PravegaWriterMode.ATLEAST_ONCE;
        txnTimeout = Time.milliseconds(DEFAULT_TXN_TIMEOUT_MILLIS);
        txnGracePeriod = Time.milliseconds(DEFAULT_TX_SCALE_GRACE_MILLIS);
    }

    /**
     * Sets the writer mode to provide at-least-once or exactly-once guarantees.
     *
     * @param writerMode The writer mode of {@code BEST_EFFORT}, {@code ATLEAST_ONCE}, or {@code EXACTLY_ONCE}.
     */
    public B withWriterMode(PravegaWriterMode writerMode) {
        this.writerMode = writerMode;
        return builder();
    }

    /**
     * Sets the transaction timeout.
     *
     * When the writer mode is set to {@code EXACTLY_ONCE}, transactions are used to persist events to the Pravega stream.
     * The timeout refers to the maximum amount of time that a transaction may remain uncommitted, after which the
     * transaction will be aborted.  The default timeout is 2 hours.
     *
     * @param timeout the timeout
     */
    public B withTxnTimeout(Time timeout) {
        Preconditions.checkArgument(timeout.getSize() > 0, "The timeout must be a positive value.");
        this.txnTimeout = timeout;
        return builder();
    }

    /**
     * Sets the transaction grace period.
     *
     * The grace period is the maximum amount of time for which a transaction may
     * remain active, after a scale operation has been initiated on the underlying stream.
     *
     * @param timeout the timeout
     */
    public B withTxnGracePeriod(Time timeout) {
        Preconditions.checkArgument(timeout.getSize() > 0, "The timeout must be a positive value.");
        this.txnGracePeriod = timeout;
        return builder();
    }

    /**
     * Creates the sink function for the current builder state.
     *
     * @param serializationSchema the deserialization schema to use.
     * @param eventRouter the event router to use.
     */
    FlinkPravegaWriter<T> createSinkFunction(SerializationSchema<T> serializationSchema, PravegaEventRouter<T> eventRouter) {
        Preconditions.checkNotNull(serializationSchema, "serializationSchema");
        Preconditions.checkNotNull(eventRouter, "eventRouter");
        return new FlinkPravegaWriter<>(
                getPravegaConfig().getClientConfig(),
                resolveStream(),
                serializationSchema,
                eventRouter,
                writerMode,
                txnTimeout.toMilliseconds(),
                txnGracePeriod.toMilliseconds());
    }
}
