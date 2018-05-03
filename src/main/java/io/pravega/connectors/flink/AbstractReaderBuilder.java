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

import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.connectors.flink.util.StreamWithBoundaries;
import lombok.Data;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A base builder for connectors that consume a Pravega stream.
 * @param <B> the builder class.
 */
public abstract class AbstractReaderBuilder<B extends AbstractReaderBuilder> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<StreamSpec> streams;

    private PravegaConfig pravegaConfig;

    protected AbstractReaderBuilder() {
        this.streams = new ArrayList<>(1);
        this.pravegaConfig = PravegaConfig.fromDefaults();
    }

    /**
     * Set the Pravega client configuration, which includes connection info, security info, and a default scope.
     *
     * The default client configuration is obtained from {@code PravegaConfig.fromDefaults()}.
     * @param pravegaConfig the configuration to use.
     */
    public B withPravegaConfig(PravegaConfig pravegaConfig) {
        this.pravegaConfig = pravegaConfig;
        return builder();
    }

    /**
     * Add a stream and its associated start {@link StreamCut} to be read by the readers of a ReaderGroup.
     * @param streamSpec the unqualified or qualified name of the stream.
     * @param startStreamCut Start {@link StreamCut}
     * @return A builder to configure and create a reader.
     */
    public B forStream(final String streamSpec, final StreamCut startStreamCut) {
        streams.add(StreamSpec.of(streamSpec, startStreamCut, StreamCut.UNBOUNDED));
        return builder();
    }

    /**
     * Add a stream that needs to be read by the readers of a ReaderGroup. The current starting position of the stream
     * will be used as the starting StreamCut.
     * @param streamSpec the unqualified or qualified name of the stream.
     * @return A builder to configure and create a reader.
     */
    public B forStream(final String streamSpec) {
        return forStream(streamSpec, StreamCut.UNBOUNDED);
    }

    /**
     * Add a stream and its associated start {@link StreamCut} to be read by the readers of a ReaderGroup.
     * @param stream Stream.
     * @param startStreamCut Start {@link StreamCut}
     * @return A builder to configure and create a reader.
     */
    public B forStream(final Stream stream, final StreamCut startStreamCut) {
        streams.add(StreamSpec.of(stream, startStreamCut, StreamCut.UNBOUNDED));
        return builder();
    }

    /**
     * Add a stream that needs to be read by the readers of a ReaderGroup. The current starting position of the stream
     * will be used as the starting StreamCut.
     * @param stream Stream.
     * @return A builder to configure and create a reader.
     */
    public B forStream(final Stream stream) {
        return forStream(stream, StreamCut.UNBOUNDED);
    }

    /**
     * Gets the Pravega configuration.
     */
    protected PravegaConfig getPravegaConfig() {
        Preconditions.checkState(pravegaConfig != null, "A Pravega configuration must be supplied.");
        return pravegaConfig;
    }

    /**
     * Resolves the streams to be provided to the reader, based on the configured default scope.
     */
    protected List<StreamWithBoundaries> resolveStreams() {
        Preconditions.checkState(!streams.isEmpty(), "At least one stream must be supplied.");
        PravegaConfig pravegaConfig = getPravegaConfig();
        return streams.stream()
                .map(s -> StreamWithBoundaries.of(pravegaConfig.resolve(s.streamSpec), s.from, s.to))
                .collect(Collectors.toList());
    }

    protected abstract B builder();

    /**
     * A Pravega stream with optional boundaries based on stream cuts.
     */
    @Data
    private static class StreamSpec implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String streamSpec;
        private final StreamCut from;
        private final StreamCut to;

        public static StreamSpec of(String streamSpec, StreamCut from, StreamCut to) {
            Preconditions.checkNotNull(streamSpec, "streamSpec");
            Preconditions.checkNotNull(streamSpec, "from");
            Preconditions.checkNotNull(streamSpec, "to");
            return new StreamSpec(streamSpec, from, to);
        }

        public static StreamSpec of(Stream stream, StreamCut from, StreamCut to) {
            Preconditions.checkNotNull(stream, "stream");
            Preconditions.checkNotNull(stream, "from");
            Preconditions.checkNotNull(stream, "to");
            return new StreamSpec(stream.getScopedName(), from, to);
        }
    }

}
