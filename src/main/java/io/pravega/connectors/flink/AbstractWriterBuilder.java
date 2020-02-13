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

import io.pravega.client.stream.Stream;
import lombok.Data;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * A base builder for connectors that emit a Pravega stream.
 *
 * @param <B> the builder class.
 */
public abstract class AbstractWriterBuilder<B extends AbstractWriterBuilder> implements Serializable {

    private PravegaConfig pravegaConfig;

    private StreamSpec stream;

    private boolean enableMetrics = true;

    public AbstractWriterBuilder() {
        this.pravegaConfig = PravegaConfig.fromDefaults();
    }

    /**
     * Set the Pravega client configuration, which includes connection info, security info, and a default scope.
     * <p>
     * The default client configuration is obtained from {@code PravegaConfig.fromDefaults()}.
     *
     * @param pravegaConfig the configuration to use.
     */
    public B withPravegaConfig(PravegaConfig pravegaConfig) {
        this.pravegaConfig = pravegaConfig;
        return builder();
    }

    /**
     * Add a stream to be written to by the writer.
     *
     * @param streamSpec the unqualified or qualified name of the stream.
     * @return A builder to configure and create a writer.
     */
    public B forStream(final String streamSpec) {
        this.stream = StreamSpec.of(streamSpec);
        return builder();
    }

    /**
     * Add a stream to be written to by the writer.
     *
     * @param stream the stream.
     * @return A builder to configure and create a writer.
     */
    public B forStream(final Stream stream) {
        this.stream = StreamSpec.of(stream);
        return builder();
    }

    /**
     * Gets the Pravega configuration.
     */
    protected PravegaConfig getPravegaConfig() {
        Preconditions.checkState(pravegaConfig != null, "A Pravega configuration must be supplied.");
        return pravegaConfig;
    }

    /**
     * Resolves the stream to be provided to the writer, based on the configured default scope.
     */
    protected Stream resolveStream() {
        Preconditions.checkState(stream != null, "A stream must be supplied.");
        PravegaConfig pravegaConfig = getPravegaConfig();
        return pravegaConfig.resolve(stream.streamSpec);
    }

    /**
     * enable/disable pravega writer metrics (default: enabled).
     *
     * @param enable boolean
     * @return A builder to configure and create a writer.
     */
    public B enableMetrics(boolean enable) {
        this.enableMetrics = enable;
        return builder();
    }

    /**
     * getter to fetch the metrics flag.
     */
    protected boolean isMetricsEnabled() {
        return enableMetrics;
    }

    protected abstract B builder();

    /**
     * A Pravega stream.
     */
    @Data
    private static class StreamSpec implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String streamSpec;

        public static StreamSpec of(String streamSpec) {
            Preconditions.checkNotNull(streamSpec, "streamSpec");
            return new StreamSpec(streamSpec);
        }

        public static StreamSpec of(Stream stream) {
            Preconditions.checkNotNull(stream, "stream");
            return new StreamSpec(stream.getScopedName());
        }
    }
}
