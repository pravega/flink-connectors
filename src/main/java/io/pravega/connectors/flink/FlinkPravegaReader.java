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
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.streaming.api.checkpoint.ExternallyInducedSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.FlinkException;

import static io.pravega.connectors.flink.util.FlinkPravegaUtils.createPravegaReader;

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

    // ----- configuration fields -----

    // the uuid of the checkpoint hook, used to store state and resume existing state from savepoints
    final String hookUid;

    // The Pravega client config.
    final ClientConfig clientConfig;

    // The Pravega reader group config.
    final ReaderGroupConfig readerGroupConfig;

    // The scope name of the reader group.
    final String readerGroupScope;

    // The readergroup name to coordinate the parallel readers. This should be unique for a Flink job.
    final String readerGroupName;

    // The supplied event deserializer.
    final DeserializationSchema<T> deserializationSchema;

    // the timeout for reading events from Pravega 
    final Time eventReadTimeout;

    // the timeout for call that initiates the Pravega checkpoint 
    final Time checkpointInitiateTimeout;

    // ----- runtime fields -----

    // Flag to terminate the source. volatile, because 'stop()' and 'cancel()' 
    // may be called asynchronously 
    volatile boolean running = true;

    // checkpoint trigger callback, invoked when a checkpoint event is received.
    // no need to be volatile, the source is driven by only one thread
    private transient CheckpointTrigger checkpointTrigger;

    // ------------------------------------------------------------------------

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
     * @param hookUid                   The UID of the source hook in the job graph.
     * @param clientConfig              The Pravega client configuration.
     * @param readerGroupConfig         The Pravega reader group configuration.
     * @param readerGroupScope          The reader group scope name.
     * @param readerGroupName           The reader group name.
     * @param deserializationSchema     The implementation to deserialize events from Pravega streams.
     * @param eventReadTimeout          The event read timeout.
     * @param checkpointInitiateTimeout The checkpoint initiation timeout.
     */
    protected FlinkPravegaReader(String hookUid, ClientConfig clientConfig,
                                 ReaderGroupConfig readerGroupConfig, String readerGroupScope, String readerGroupName,
                                 DeserializationSchema<T> deserializationSchema, Time eventReadTimeout, Time checkpointInitiateTimeout) {

        this.hookUid = Preconditions.checkNotNull(hookUid, "hookUid");
        this.clientConfig = Preconditions.checkNotNull(clientConfig, "clientConfig");
        this.readerGroupConfig = Preconditions.checkNotNull(readerGroupConfig, "readerGroupConfig");
        this.readerGroupScope = Preconditions.checkNotNull(readerGroupScope, "readerGroupScope");
        this.readerGroupName = Preconditions.checkNotNull(readerGroupName, "readerGroupName");
        this.deserializationSchema = Preconditions.checkNotNull(deserializationSchema, "deserializationSchema");
        this.eventReadTimeout = Preconditions.checkNotNull(eventReadTimeout, "eventReadTimeout");
        this.checkpointInitiateTimeout = Preconditions.checkNotNull(checkpointInitiateTimeout, "checkpointInitiateTimeout");
    }

    /**
     * Initializes the reader.
     */
    void initialize() {
        // TODO: This will require the client to have access to the pravega controller and handle any temporary errors.
        //       See https://github.com/pravega/flink-connectors/issues/130.
        log.info("Creating reader group: {}/{} for the Flink job", this.readerGroupScope, this.readerGroupName);
        createReaderGroup();
    }

    // ------------------------------------------------------------------------
    //  source function methods
    // ------------------------------------------------------------------------

    @Override
    public void run(SourceContext<T> ctx) throws Exception {

        final String readerId = getRuntimeContext().getTaskNameWithSubtasks();

        log.info("{} : Creating Pravega reader with ID '{}' for controller URI: {}",
                getRuntimeContext().getTaskNameWithSubtasks(), readerId, this.clientConfig.getControllerURI());

        try (EventStreamReader<T> pravegaReader = createEventStreamReader(readerId)) {

            log.info("Starting Pravega reader '{}' for controller URI {}", readerId, this.clientConfig.getControllerURI());

            // main work loop, which this task is running
            while (this.running) {
                final EventRead<T> eventRead = pravegaReader.readNextEvent(eventReadTimeout.toMilliseconds());
                final T event = eventRead.getEvent();

                // emit the event, if one was carried
                if (event != null) {
                    if (this.deserializationSchema.isEndOfStream(event)) {
                        // Found stream end marker.
                        // TODO: Handle scenario when reading from multiple segments. This will be cleaned up as part of:
                        //       https://github.com/pravega/pravega/issues/551.
                        log.info("Reached end of stream for reader: {}", readerId);
                        return;
                    }

                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(event);
                    }
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
        return new ReaderCheckpointHook(this.hookUid, createReaderGroup(), this.checkpointInitiateTimeout);
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
    //  utility
    // ------------------------------------------------------------------------

    /**
     * Create the {@link ReaderGroup} for the current configuration.
     */
    protected ReaderGroup createReaderGroup() {
        ReaderGroupManager readerGroupManager = createReaderGroupManager();
        readerGroupManager.createReaderGroup(this.readerGroupName, readerGroupConfig);
        return readerGroupManager.getReaderGroup(this.readerGroupName);
    }

    /**
     * Create the {@link ReaderGroupManager} for the current configuration.
     */
    protected ReaderGroupManager createReaderGroupManager() {
        return ReaderGroupManager.withScope(readerGroupScope, clientConfig);
    }

    /**
     * Create the {@link EventStreamReader} for the current configuration.
     * @param readerId the readerID to use.
     */
    protected EventStreamReader<T> createEventStreamReader(String readerId) {
        return createPravegaReader(
                this.clientConfig,
                readerId,
                this.readerGroupScope,
                this.readerGroupName,
                this.deserializationSchema,
                ReaderConfig.builder().build());
    }

    // ------------------------------------------------------------------------
    //  configuration
    // ------------------------------------------------------------------------

    /**
     * Gets a builder for {@link FlinkPravegaReader} to read Pravega streams using the Flink streaming API.
     * @param <T> the element type.
     */
    public static <T> FlinkPravegaReader.Builder<T> builder() {
        return new Builder<>();
    }

    /**
     * A builder for {@link FlinkPravegaReader}.
     *
     * @param <T> the element type.
     */
    public static class Builder<T> extends AbstractStreamingReaderBuilder<T, Builder<T>> {

        private DeserializationSchema<T> deserializationSchema;

        protected Builder<T> builder() {
            return this;
        }

        /**
         * Sets the deserialization schema.
         *
         * @param deserializationSchema The deserialization schema
         */
        public Builder<T> withDeserializationSchema(DeserializationSchema<T> deserializationSchema) {
            this.deserializationSchema = deserializationSchema;
            return builder();
        }

        @Override
        protected DeserializationSchema<T> getDeserializationSchema() {
            Preconditions.checkState(deserializationSchema != null, "Deserialization schema must not be null.");
            return deserializationSchema;
        }

        /**
         * Builds a {@link FlinkPravegaReader} based on the configuration.
         * @throws IllegalStateException if the configuration is invalid.
         */
        public FlinkPravegaReader<T> build() {
            FlinkPravegaReader<T> reader = buildSourceFunction();
            reader.initialize();
            return reader;
        }
    }
}