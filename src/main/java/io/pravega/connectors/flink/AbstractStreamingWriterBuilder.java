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

    // the numbers below are picked based on the default max settings in Pravega
    protected static final long DEFAULT_TXN_LEASE_RENEWAL_PERIOD_MILLIS = 30000; // 30 seconds

    protected PravegaWriterMode writerMode;
    protected boolean enableWatermark;
    protected Time txnLeaseRenewalPeriod;

    protected AbstractStreamingWriterBuilder() {
        writerMode = PravegaWriterMode.ATLEAST_ONCE;
        enableWatermark = false;
        txnLeaseRenewalPeriod = Time.milliseconds(DEFAULT_TXN_LEASE_RENEWAL_PERIOD_MILLIS);
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
     * Enable watermark.
     *
     * @param enableWatermark boolean
     */
    public B enableWatermark(boolean enableWatermark) {
        this.enableWatermark = enableWatermark;
        return builder();
    }

    /**
     * Sets the transaction lease renewal period.
     *
     * When the writer mode is set to {@code EXACTLY_ONCE}, transactions are used to persist
     * events to the Pravega stream.  The transaction interval corresponds to the Flink checkpoint interval.
     * Throughout that interval, the transaction is kept alive with a lease that is periodically renewed.
     * This configuration setting sets the lease renewal period.  The default value is 30 seconds.
     *
     * @param period the lease renewal period
     */
    public B withTxnLeaseRenewalPeriod(Time period) {
        Preconditions.checkArgument(period.getSize() > 0, "The timeout must be a positive value.");
        this.txnLeaseRenewalPeriod = period;
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
                txnLeaseRenewalPeriod.toMilliseconds(),
                enableWatermark,
                isMetricsEnabled());
    }
}
