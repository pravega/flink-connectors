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

import com.google.common.base.Preconditions;

import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;

import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import java.io.IOException;
import java.net.URI;
import java.util.Set;

import static io.pravega.connectors.flink.util.FlinkPravegaUtils.createPravegaReader;
import static io.pravega.connectors.flink.util.FlinkPravegaUtils.generateRandomReaderGroupName;

/**
 * A Flink {@link InputFormat} that can be added as a source to read from Pravega in a Flink batch job.
 */
public class FlinkPravegaInputFormat<T> extends GenericInputFormat<T> {

    private static final long serialVersionUID = 1L;

    private static final long DEFAULT_EVENT_READ_TIMEOUT = 1000;

    // The supplied event deserializer.
    private final DeserializationSchema<T> deserializationSchema;

    // The pravega controller endpoint.
    private final URI controllerURI;

    // The scope name of the destination stream.
    private final String scopeName;

    // The readergroup name to coordinate the parallel readers. This should be unique for a Flink job.
    private final String readerGroupName;

    // The names of Pravega streams to read
    private final Set<String> streamNames;

    // The configured start time for the reader
    private final long startTime;

    // The event read timeout
    private long eventReadTimeout = DEFAULT_EVENT_READ_TIMEOUT;

    // The Pravega reader; a new reader will be opened for each input split
    private transient EventStreamReader<T> pravegaReader;

    // Read-ahead event; null indicates that end of input is reached
    private transient EventRead<T> lastReadAheadEvent;

    /**
     * Creates a new Flink Pravega {@link InputFormat} which can be added as a source to a Flink batch job.
     *
     * <p>The number of created input splits is equivalent to the parallelism of the source. For each input split,
     * a Pravega reader will be created to read from the specified Pravega streams. Each input split is closed when
     * the next read event returns {@code null} on {@link EventRead#getEvent()}.
     *
     * @param controllerURI         The pravega controller endpoint address.
     * @param scope                 The destination stream's scope name.
     * @param streamNames           The list of stream names to read events from.
     * @param startTime             The start time from when to read events from.
     *                              Use 0 to read all stream events from the beginning.
     * @param deserializationSchema The implementation to deserialize events from pravega streams.
     */
    public FlinkPravegaInputFormat(
            final URI controllerURI,
            final String scope,
            final Set<String> streamNames,
            final long startTime,
            final DeserializationSchema<T> deserializationSchema) {

        Preconditions.checkNotNull(controllerURI, "controllerURI");
        Preconditions.checkNotNull(scope, "scope");
        Preconditions.checkNotNull(streamNames, "streamNames");
        Preconditions.checkArgument(startTime >= 0, "start time must be >= 0");
        Preconditions.checkNotNull(deserializationSchema, "deserializationSchema");

        this.controllerURI = controllerURI;
        this.scopeName = scope;
        this.deserializationSchema = deserializationSchema;
        this.streamNames = streamNames;
        this.startTime = startTime;
        this.readerGroupName = generateRandomReaderGroupName();

        // TODO: This will require the client to have access to the pravega controller and handle any temporary errors.
        ReaderGroupManager.withScope(scopeName, controllerURI)
                .createReaderGroup(this.readerGroupName, ReaderGroupConfig.builder().startingTime(startTime).build(),
                        streamNames);
    }

    // ------------------------------------------------------------------------
    //  User configurations
    // ------------------------------------------------------------------------

    /**
     * Gets the timeout for the call to read events from Pravega. After the timeout
     * expires (without an event being returned), another call will be made.
     *
     * <p>This timeout is passed to {@link EventStreamReader#readNextEvent(long)}.
     *
     * @param eventReadTimeout The timeout, in milliseconds
     */
    public void setEventReadTimeout(long eventReadTimeout) {
        Preconditions.checkArgument(eventReadTimeout > 0, "timeout must be >= 0");
        this.eventReadTimeout = eventReadTimeout;
    }

    /**
     * Gets the timeout for the call to read events from Pravega.
     *
     * <p>This timeout is the value passed to {@link EventStreamReader#readNextEvent(long)}.
     *
     * @return The timeout, in milliseconds
     */
    public long getEventReadTimeout() {
        return eventReadTimeout;
    }

    // ------------------------------------------------------------------------
    //  Input split life cycle methods
    // ------------------------------------------------------------------------

    @Override
    public void open(GenericInputSplit split) throws IOException {
        super.open(split);

        // build a new reader for each input split
        this.pravegaReader = createPravegaReader(
                this.scopeName,
                this.controllerURI,
                getRuntimeContext().getTaskNameWithSubtasks(),
                this.readerGroupName,
                this.deserializationSchema,
                ReaderConfig.builder().build());
    }

    @Override
    public boolean reachedEnd() throws IOException {
        // look ahead to see if we have reached the end of input
        try {
            this.lastReadAheadEvent = pravegaReader.readNextEvent(eventReadTimeout);
        } catch (Exception e) {
            throw new IOException("Failed to read next event.", e);
        }

        // TODO this "end of input" marker is too brittle, as the timeout could easily be a temporary hiccup;
        // TODO to make this more robust, we could loop and try to fetch a few more times before concluding end of input
        return lastReadAheadEvent.getEvent() == null;
    }

    @Override
    public T nextRecord(T t) throws IOException {
        // reachedEnd() will be checked first, so lastReadAheadEvent should never be null
        return lastReadAheadEvent.getEvent();
    }

    @Override
    public void close() throws IOException {
        this.pravegaReader.close();
    }
}
