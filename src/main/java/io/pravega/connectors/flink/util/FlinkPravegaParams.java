/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink.util;

import com.google.common.collect.Sets;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.connectors.flink.serialization.PravegaSerialization;
import java.io.Serializable;
import java.net.URI;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * Convenience class for extracting pravega parameters from flink job parameters.
 *
 * As a convention, the pravega controller uri will be passed as a parameter named 'controller'.
 *
 * Operations are provided to create streams, readers and writers based on stream names as
 * parameters.
 *
 * @see StreamId
 */
public class FlinkPravegaParams {
    public static final String DEFAULT_CONTROLLER_URI = "tcp://127.0.0.1:9090";
    public static final String CONTROLLER_PARAM_NAME = "controller";

    private ParameterTool params;

    public FlinkPravegaParams(ParameterTool params) {
        this.params = params;
    }

    /**
     * Gets the controller URI from the 'controller' job parameter. If this parameter
     * is not specified this defaults to DEFAULT_CONTROLLER_URI.
     */
    public URI getControllerUri() {
        return URI.create(params.get(CONTROLLER_PARAM_NAME, DEFAULT_CONTROLLER_URI));
    }

    /**
     * Gets a StreamId from the flink job parameters.
     *
     * @param streamParamName Parameter name that contains the stream
     * @param defaultStreamSpec Default stream in the format [scope]/[stream] if the parameter was not found.
     * @return stream found in the parameter or the default if no parameter is found.
     */
    public StreamId getStreamFromParam(final String streamParamName, final String defaultStreamSpec) {
        return StreamId.fromSpec(params.get(streamParamName, defaultStreamSpec));
    }

    /**
     * Constructs a new reader using stream/scope name from job parameters. Uses PravegaSerialization to only require
     * event class type to be specified.
     *
     * @param stream Stream to read from.
     * @param startTime The start time from when to read events from. Use 0 to read all stream events from the beginning.
     * @param eventType Class type for events on this stream.
     * @param <T> Type for events on this stream.
     * @see PravegaSerialization
     */
    public <T extends Serializable> FlinkPravegaReader<T> newReader(final StreamId stream,
                                                                    final long startTime,
                                                                    final Class<T> eventType) {
        return newReader(stream, startTime, PravegaSerialization.deserializationFor(eventType));
    }

    /**
     * Constructs a new reader using stream/scope name from job parameters.
     *
     * @param stream Stream to read from.
     * @param startTime The start time from when to read events from. Use 0 to read all stream events from the beginning.
     * @param deserializationSchema The implementation to deserialize events from pravega streams.
     * @param <T> Type for events on this stream.
     */
    public <T extends Serializable> FlinkPravegaReader<T> newReader(final StreamId stream,
                                                                    final long startTime,
                                                                    final DeserializationSchema<T> deserializationSchema) {
        return new FlinkPravegaReader<>(getControllerUri(), stream.getScope(), Sets.newHashSet(stream.getName()),
                startTime, deserializationSchema);
    }

    /**
     * Constructs a new writer using stream/scope name from job parameters. Uses PravegaSerialization to only require
     * event class type to be specified.
     *
     * @param stream Stream to read from.
     * @param eventType Class type for events on this stream.
     * @param router The implementation to extract the partition key from the event.
     * @param <T> Type for events on this stream.
     * @see PravegaSerialization
     */
    public <T extends Serializable> FlinkPravegaWriter<T> newWriter(final StreamId stream,
                                                                    final Class<T> eventType,
                                                                    final PravegaEventRouter<T> router) {
        return newWriter(stream, PravegaSerialization.serializationFor(eventType), router);
    }

    /**
     * Constructs a new writer using stream/scope name from job parameters.
     *
     * @param stream Stream to read from.
     * @param serializationSchema The implementation for serializing every event into pravega's storage format.
     * @param router The implementation to extract the partition key from the event.
     * @param <T> Type for events on this stream.
     */
    public <T extends Serializable> FlinkPravegaWriter<T> newWriter(final StreamId stream,
                                                                    final SerializationSchema<T> serializationSchema,
                                                                    final PravegaEventRouter<T> router) {
        return new FlinkPravegaWriter<>(getControllerUri(), stream.getScope(), stream.getName(),
                serializationSchema, router);
    }

    /**
     * Ensures a stream is created.
     *
     * @param streamParamName Parameter name that contains the stream
     * @param defaultStreamSpec Default stream in the format [scope]/[stream]
     */
    public StreamId createStreamFromParam(final String streamParamName, final String defaultStreamSpec) {
        StreamId streamId = getStreamFromParam(streamParamName, defaultStreamSpec);
        createStream(streamId);
        return streamId;
    }

    public void createStream(final StreamId streamId) {
        createStream(streamId, ScalingPolicy.fixed(1));
    }

    public void createStream(final StreamId streamId, final ScalingPolicy scalingPolicy) {
        StreamManager streamManager = StreamManager.create(getControllerUri());
        streamManager.createScope(streamId.getScope());

        StreamConfiguration streamConfig = StreamConfiguration.builder().scalingPolicy(scalingPolicy).build();
        streamManager.createStream(streamId.getScope(), streamId.getName(), streamConfig);
    }
}
