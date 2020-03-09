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

import com.google.common.base.Preconditions;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.connectors.flink.util.FlinkPravegaUtils;
import io.pravega.connectors.flink.watermark.AssignerWithTimeWindows;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.SerializedValue;

import java.util.Optional;

/**
 * An abstract streaming reader builder.
 *
 * The builder is abstracted to act as the base for both the {@link FlinkPravegaReader} and {@link FlinkPravegaTableSource} builders.
 *
 * @param <T> the element type.
 * @param <B> the builder type.
 */
abstract class AbstractStreamingReaderBuilder<T, B extends AbstractStreamingReaderBuilder> extends AbstractReaderBuilder<B> {

    private static final Time DEFAULT_EVENT_READ_TIMEOUT = Time.seconds(1);
    private static final Time DEFAULT_CHECKPOINT_INITIATE_TIMEOUT = Time.seconds(5);
    private static final int  DEFAULT_MAX_OUTSTANDING_CHECKPOINT_REQUEST = 3;

    protected String uid;
    protected String readerGroupScope;
    protected String readerGroupName;
    protected Time readerGroupRefreshTime;
    protected Time checkpointInitiateTimeout;
    protected Time eventReadTimeout;
    protected int maxOutstandingCheckpointRequest;

    protected AbstractStreamingReaderBuilder() {
        this.checkpointInitiateTimeout = DEFAULT_CHECKPOINT_INITIATE_TIMEOUT;
        this.eventReadTimeout = DEFAULT_EVENT_READ_TIMEOUT;
        this.maxOutstandingCheckpointRequest = DEFAULT_MAX_OUTSTANDING_CHECKPOINT_REQUEST;
    }

    /**
     * Configures the source uid to identify the checkpoint state of this source.
     * <p>
     * The default value is generated based on other inputs.
     *
     * @param uid the uid to use.
     */
    public B uid(String uid) {
        this.uid = uid;
        return builder();
    }

    /**
     * Configures the reader group scope for synchronization purposes.
     * <p>
     * The default value is taken from the {@link PravegaConfig} {@code defaultScope} property.
     *
     * @param scope the scope name.
     */
    public B withReaderGroupScope(String scope) {
        this.readerGroupScope = Preconditions.checkNotNull(scope);
        return builder();
    }

    /**
     * Configures the reader group name.
     *
     * @param readerGroupName the reader group name.
     */
    public B withReaderGroupName(String readerGroupName) {
        this.readerGroupName = Preconditions.checkNotNull(readerGroupName);
        return builder();
    }

    /**
     * Sets the group refresh time, with a default of 1 second.
     *
     * @param groupRefreshTime The group refresh time
     */
    public B withReaderGroupRefreshTime(Time groupRefreshTime) {
        this.readerGroupRefreshTime = groupRefreshTime;
        return builder();
    }

    /**
     * Sets the timeout for initiating a checkpoint in Pravega.
     *
     * @param checkpointInitiateTimeout The timeout
     */
    public B withCheckpointInitiateTimeout(Time checkpointInitiateTimeout) {
        Preconditions.checkArgument(checkpointInitiateTimeout.getSize() > 0, "timeout must be > 0");
        this.checkpointInitiateTimeout = checkpointInitiateTimeout;
        return builder();
    }

    /**
     * Sets the timeout for the call to read events from Pravega. After the timeout
     * expires (without an event being returned), another call will be made.
     *
     * @param eventReadTimeout The timeout
     */
    public B withEventReadTimeout(Time eventReadTimeout) {
        Preconditions.checkArgument(eventReadTimeout.getSize() > 0, "timeout must be > 0");
        this.eventReadTimeout = eventReadTimeout;
        return builder();
    }

    /**
     * Configures the maximum outstanding checkpoint requests to Pravega (default=3).
     * Upon requesting more checkpoints than the specified maximum,
     * (say a checkpoint request times out on the ReaderCheckpointHook but Pravega is still working on it),
     * this configurations allows Pravega to limit any further checkpoint request being made to the ReaderGroup.
     * This configuration is particularly relevant when multiple checkpoint requests need to be honored (e.g., frequent savepoint requests being triggered concurrently).
     *
     * @param maxOutstandingCheckpointRequest maximum outstanding checkpoint request.
     */
    public B withMaxOutstandingCheckpointRequest(int maxOutstandingCheckpointRequest) {
        this.maxOutstandingCheckpointRequest = maxOutstandingCheckpointRequest;
        return builder();
    }

    protected abstract DeserializationSchema<T> getDeserializationSchema();

    protected abstract SerializedValue<AssignerWithTimeWindows<T>> getAssignerWithTimeWindows();

    /**
     * Builds a {@link FlinkPravegaReader} based on the configuration.
     *
     * Note that the {@link FlinkPravegaTableSource} supports both the batch and streaming API, and so creates both
     * a source function and an input format and then uses one or the other.
     *
     * Be sure to call {@code initialize()} before returning the reader to user code.
     *
     * @throws IllegalStateException if the configuration is invalid.
     * @return an uninitiailized reader as a source function.
     */
    FlinkPravegaReader<T> buildSourceFunction() {
        ReaderGroupInfo readerGroupInfo = buildReaderGroupInfo();
        return new FlinkPravegaReader<>(
                Optional.ofNullable(this.uid).orElseGet(this::generateUid),
                getPravegaConfig().getClientConfig(),
                readerGroupInfo.getReaderGroupConfig(),
                readerGroupInfo.getReaderGroupScope(),
                readerGroupInfo.getReaderGroupName(),
                getDeserializationSchema(),
                getAssignerWithTimeWindows(),
                this.eventReadTimeout,
                this.checkpointInitiateTimeout,
                isMetricsEnabled());
    }

    /**
     * Build reader group configuration
     *
     * @return {@link ReaderGroupInfo}
     */
    ReaderGroupInfo buildReaderGroupInfo() {
        // rgConfig
        ReaderGroupConfig.ReaderGroupConfigBuilder rgConfigBuilder = ReaderGroupConfig
                .builder()
                .maxOutstandingCheckpointRequest(this.maxOutstandingCheckpointRequest)
                .disableAutomaticCheckpoints();
        if (this.readerGroupRefreshTime != null) {
            rgConfigBuilder.groupRefreshTimeMillis(this.readerGroupRefreshTime.toMilliseconds());
        }
        resolveStreams().forEach(s -> rgConfigBuilder.stream(s.getStream(), s.getFrom(), s.getTo()));
        final ReaderGroupConfig rgConfig = rgConfigBuilder.build();

        // rgScope
        final String rgScope = Optional.ofNullable(this.readerGroupScope).orElseGet(() -> {
            Preconditions.checkState(getPravegaConfig().getDefaultScope() != null,  "A reader group scope or default scope must be configured");
            return getPravegaConfig().getDefaultScope();
        });

        // rgName
        final String rgName = Optional.ofNullable(this.readerGroupName).orElseGet(FlinkPravegaUtils::generateRandomReaderGroupName);
        return new ReaderGroupInfo(rgConfig, rgScope, rgName);
    }

    /**
     * Generate a UID for the source, to distinguish the state associated with the checkpoint hook.  A good generated UID will:
     * 1. be stable across savepoints for the same inputs
     * 2. disambiguate one source from another (e.g. in a program that uses numerous instances of {@link FlinkPravegaReader})
     * 3. allow for reconfiguration of the stream cuts and timeouts
     */
    String generateUid() {
        StringBuilder sb = new StringBuilder();
        sb.append(readerGroupScope).append('\n');
        resolveStreams().forEach(s -> sb
                .append(s.getStream().getScopedName())
                .append('\n'));
        return Integer.toString(sb.toString().hashCode());
    }

    static class ReaderGroupInfo {
        private final ReaderGroupConfig readerGroupConfig;
        private final String readerGroupScope;
        private final String readerGroupName;

        public ReaderGroupInfo(ReaderGroupConfig rgConfig, String rgScope, String rgName) {
            this.readerGroupConfig = rgConfig;
            this.readerGroupScope = rgScope;
            this.readerGroupName = rgName;
        }

        public ReaderGroupConfig getReaderGroupConfig() {
            return readerGroupConfig;
        }

        public String getReaderGroupScope() {
            return readerGroupScope;
        }

        public String getReaderGroupName() {
            return readerGroupName;
        }
    }
}
