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
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;

import io.pravega.client.stream.TimeDomain;
import lombok.extern.slf4j.Slf4j;

import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.streaming.api.checkpoint.ExternallyInducedSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.TimestampAssigner;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.util.FlinkException;

import java.net.URI;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import static io.pravega.connectors.flink.util.FlinkPravegaUtils.createPravegaReader;
import static io.pravega.connectors.flink.util.FlinkPravegaUtils.generateRandomReaderGroupName;
import static io.pravega.connectors.flink.util.FlinkPravegaUtils.getDefaultReaderName;

/**
 * Flink source implementation for reading from pravega storage.
 *
 * @param <T> The type of the event to be written.
 */
@Slf4j
public class FlinkPravegaReader<T>
        extends RichParallelSourceFunction<T>
        implements ResultTypeQueryable<T>, StoppableFunction, ExternallyInducedSource<T, Checkpoint> {

    private static final long serialVersionUID = 1L;

    private static final long DEFAULT_EVENT_READ_TIMEOUT = 1000;

    private static final long DEFAULT_CHECKPOINT_INITIATE_TIMEOUT = 5000;

    private static final Duration DEFAULT_EVENT_TIME_LAG = Duration.ofMinutes(1);

    // ----- configuration fields -----

    // The supplied event deserializer.
    private final DeserializationSchema<T> deserializationSchema;

    // The pravega controller endpoint.
    private final URI controllerURI;

    // The scope name of the destination stream.
    private final String scopeName;

    // The readergroup name to coordinate the parallel readers. This should be unique for a Flink job.
    private final String readerGroupName;

    // the name of the reader, used to store state and resume existing state from savepoints
    private final String readerName;

    // the timeout for reading events from Pravega 
    private long eventReadTimeout = DEFAULT_EVENT_READ_TIMEOUT;

    // the timeout for call that initiates the Pravega checkpoint 
    private long checkpointInitiateTimeout = DEFAULT_CHECKPOINT_INITIATE_TIMEOUT;

    // the maximum event time lag for watermarking purposes
    private Duration eventTimeLag = DEFAULT_EVENT_TIME_LAG;

    // the (optional) timestamp assigner
    private TimestampAssigner<T> timestampAssigner;

    // ----- runtime fields -----

    // Flag to terminate the source. volatile, because 'stop()' and 'cancel()' 
    // may be called asynchronously 
    private volatile boolean running = true;

    // checkpoint trigger callback, invoked when a checkpoint event is received.
    // no need to be volatile, the source is driven by only one thread
    private transient CheckpointTrigger checkpointTrigger;

    // ------------------------------------------------------------------------

    /**
     * Creates a new Flink Pravega reader instance which can be added as a source to a Flink job.
     *
     * <p>The reader will use a random name under which it stores its state in a checkpoint. While
     * checkpoints still work, this means that matching the state into another Flink jobs
     * (when resuming from a savepoint) will not be possible. Thus it is generally recommended
     * to give a reader name to each reader.
     *
     * @param controllerURI         The pravega controller endpoint address.
     * @param scope                 The destination stream's scope name.
     * @param streamNames           The list of stream names to read events from.
     * @param startTime             The start time from when to read events from.
     *                              Use 0 to read all stream events from the beginning.
     * @param deserializationSchema The implementation to deserialize events from pravega streams.
     */
    public FlinkPravegaReader(final URI controllerURI, final String scope, final Set<String> streamNames,
                              final long startTime, final DeserializationSchema<T> deserializationSchema) {

        this(controllerURI, scope, streamNames, startTime, deserializationSchema, null);
    }

    /**
     * Creates a new Flink Pravega reader instance which can be added as a source to a Flink job.
     *
     * <p>The reader will use the given {@code readerName} to store its state (its positions
     * in the stream segments) in Flink's checkpoints/savepoints. This name is used in a similar
     * way as the operator UIDs ({@link SingleOutputStreamOperator#uid(String)}) to identify state
     * when matching it into another job that resumes from this job's checkpoints/savepoints.
     *
     * <p>Without specifying a {@code readerName}, the job will correctly checkpoint and recover,
     * but new instances of the job can typically not resume this reader's state (positions).
     *
     * @param controllerURI         The pravega controller endpoint address.
     * @param scope                 The destination stream's scope name.
     * @param streamNames           The list of stream names to read events from.
     * @param startTime             The start time from when to read events from.
     *                              Use 0 to read all stream events from the beginning.
     * @param deserializationSchema The implementation to deserialize events from pravega streams.
     * @param readerName            The name of the reader, used to store state and resume existing
     *                              state from savepoints.
     */
    public FlinkPravegaReader(final URI controllerURI, final String scope, final Set<String> streamNames,
                              final long startTime, final DeserializationSchema<T> deserializationSchema,
                              final String readerName) {

        Preconditions.checkNotNull(controllerURI, "controllerURI");
        Preconditions.checkNotNull(scope, "scope");
        Preconditions.checkNotNull(streamNames, "streamNames");
        Preconditions.checkArgument(startTime >= 0, "start time must be >= 0");
        Preconditions.checkNotNull(deserializationSchema, "deserializationSchema");
        if (readerName == null) {
            this.readerName = getDefaultReaderName(scope, streamNames);
        } else {
            this.readerName = readerName;
        }

        this.controllerURI = controllerURI;
        this.scopeName = scope;
        this.deserializationSchema = deserializationSchema;
        this.readerGroupName = generateRandomReaderGroupName();

        // TODO: This will require the client to have access to the pravega controller and handle any temporary errors.
        //       See https://github.com/pravega/pravega/issues/553.
        log.info("Creating reader group: {} for the Flink job", this.readerGroupName);

        ReaderGroupManager.withScope(scope, controllerURI)
                .createReaderGroup(this.readerGroupName, ReaderGroupConfig.builder().startingTime(startTime).build(),
                        streamNames);
    }

    // ------------------------------------------------------------------------
    //  properties
    // ------------------------------------------------------------------------

    /**
     * Sets the timeout for initiating a checkpoint in Pravega.
     *
     * <p>This timeout if applied to the future returned by
     * {@link io.pravega.client.stream.ReaderGroup#initiateCheckpoint(String, ScheduledExecutorService)}.
     *
     * @param checkpointInitiateTimeout The timeout, in milliseconds
     */
    public void setCheckpointInitiateTimeout(long checkpointInitiateTimeout) {
        Preconditions.checkArgument(checkpointInitiateTimeout > 0, "timeout must be >= 0");
        this.checkpointInitiateTimeout = checkpointInitiateTimeout;
    }

    /**
     * Gets the timeout for initiating a checkpoint in Pravega.
     *
     * <p>This timeout if applied to the future returned by
     * {@link io.pravega.client.stream.ReaderGroup#initiateCheckpoint(String, ScheduledExecutorService)}.
     *
     * @return The timeout, in milliseconds
     */
    public long getCheckpointInitiateTimeout() {
        return checkpointInitiateTimeout;
    }

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

    /**
     * Sets the event time lag for watermarking purposes.
     *
     * <p>This duration represents the maximum amount of time allotted for events to be written to the Pravega stream,
     * before being considered a 'late' event.  The default lag is one minute.
     *
     * <p>If you're using transactions or exactly-once semantics to write to the stream, be sure to incorporate
     * the maximum transaction time into your lag time.
     *
     * @param eventTimeLag the lag to use.
     */
    public void setEventTimeLag(Duration eventTimeLag) {
        Preconditions.checkArgument(!eventTimeLag.isNegative(), "eventTimeLag must be non-negative");
        this.eventTimeLag = eventTimeLag;
    }

    /**
     * Gets the event time lag for watermarking purposes.
     */
    public Duration getEventTimeLag() {
        return this.eventTimeLag;
    }

    /**
     * Sets the timestamp assigner for event timestamping purposes.
     *
     * <p>The assigner is invoked per-event to produce an associated timestamp.
     *
     * @param timestampAssigner the assigner to use, or null.
     */
    public void setTimestampAssigner(TimestampAssigner<T> timestampAssigner) {
        this.timestampAssigner = timestampAssigner;
    }

    // ------------------------------------------------------------------------
    //  source function methods
    // ------------------------------------------------------------------------

    @Override
    public void run(SourceContext<T> ctx) throws Exception {

        final String readerId = getRuntimeContext().getTaskNameWithSubtasks();

        log.info("{} : Creating Pravega reader with ID '{}' for controller URI: {}",
                getRuntimeContext().getTaskNameWithSubtasks(), readerId, this.controllerURI);

        try (EventStreamReader<T> pravegaReader = createPravegaReader(
                this.scopeName,
                this.controllerURI,
                readerId,
                this.readerGroupName,
                this.deserializationSchema,
                ReaderConfig.builder().timeDomain(TimeDomain.IngestionTime).build())) {

            log.info("Starting Pravega reader '{}' for controller URI {}", readerId, this.controllerURI);

            // main work loop, which this task is running
            while (this.running) {
                final EventRead<T> eventRead = pravegaReader.readNextEvent(eventReadTimeout);
                final T event = eventRead.getEvent();

                // emit the event, if one was carried
                if (eventRead.isEvent() && event != null) {
                    if (this.deserializationSchema.isEndOfStream(event)) {
                        // Found stream end marker.
                        // TODO: Handle scenario when reading from multiple segments. This will be cleaned up as part of:
                        //       https://github.com/pravega/pravega/issues/551.
                        log.info("Reached end of stream for reader: {}", readerId);
                        return;
                    }

                    if(timestampAssigner != null) {
                        ctx.collectWithTimestamp(event, timestampAssigner.extractTimestamp(event, Long.MIN_VALUE));
                    }
                    else {
                        ctx.collect(event);
                    }
                }

                if(eventRead.isWatermark()) {
                    Watermark watermark = calculateEventTimeWatermark(eventRead.getWatermark());
                    log.debug("Advancing the event time watermark: {}", watermark);
                    ctx.emitWatermark(watermark);
                }

                // if the read marks a checkpoint, trigger the checkpoint
                if (eventRead.isCheckpoint()) {
                    triggerCheckpoint(eventRead.getCheckpointName());
                }
            }
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }

    @Override
    public void stop() {
        this.running = false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return this.deserializationSchema.getProducedType();
    }

    // ------------------------------------------------------------------------
    //  checkpoints
    // ------------------------------------------------------------------------

    @Override
    public MasterTriggerRestoreHook<Checkpoint> createMasterTriggerRestoreHook() {
        //Issue-24
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        try {
            return new ReaderCheckpointHook(this.readerName, this.readerGroupName,
                    this.scopeName, this.controllerURI, this.checkpointInitiateTimeout);
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    @Override
    public void setCheckpointTrigger(CheckpointTrigger checkpointTrigger) {
        this.checkpointTrigger = checkpointTrigger;
    }

    /**
     * Triggers the checkpoint in the Flink source operator.
     *
     * <p>This method assumes that the {@code checkpointIdentifier} is a string of the form
     */
    private void triggerCheckpoint(String checkpointIdentifier) throws FlinkException {
        Preconditions.checkState(checkpointTrigger != null, "checkpoint trigger not set");

        log.debug("{} received checkpoint event for {}",
                getRuntimeContext().getTaskNameWithSubtasks(), checkpointIdentifier);

        final long checkpointId;
        try {
            checkpointId = ReaderCheckpointHook.parseCheckpointId(checkpointIdentifier);
        } catch (IllegalArgumentException e) {
            throw new FlinkException("Cannot trigger checkpoint due to invalid Pravega checkpoint name", e.getCause());
        }

        checkpointTrigger.triggerCheckpoint(checkpointId);
    }

    // ------------------------------------------------------------------------
    //  time management
    // ------------------------------------------------------------------------

    /**
     * Converts an ingestion-time watermark to an application-specific event-time watermark.
     *
     * @param ingestionTimeWatermark the ingestion watermark read from the Pravega stream.
     * @return an event-time watermark
     */
    private Watermark calculateEventTimeWatermark(long ingestionTimeWatermark) {
        return new Watermark(ingestionTimeWatermark - eventTimeLag.toMillis());
    }
}