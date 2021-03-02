/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink.source;

import io.pravega.client.ClientConfig;
import io.pravega.client.stream.*;
import io.pravega.connectors.flink.AbstractStreamingReaderBuilder;
import io.pravega.connectors.flink.CheckpointSerializer;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.watermark.AssignerWithTimeWindows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import java.io.IOException;
import java.util.List;

@Slf4j
@PublicEvolving
public class FlinkPravegaSource<T>
        implements Source<T, PravegaSplit, Checkpoint>, ResultTypeQueryable<T> {

    final ClientConfig clientConfig;
    // The Pravega reader group config.
    final ReaderGroupConfig readerGroupConfig;

    // The scope name of the reader group.
    final String scope;

    // The readergroup name to coordinate the parallel readers. This should be unique for a Flink job.
    final String readerGroupName;

    // The supplied event deserializer.
    final DeserializationSchema<T> deserializationSchema;

    // the timeout for reading events from Pravega
    final Time eventReadTimeout;

    // the timeout for call that initiates the Pravega checkpoint
    final Time checkpointInitiateTimeout;

    // flag to enable/disable metrics
    final boolean enableMetrics;

    public FlinkPravegaSource(ClientConfig clientConfig,
                                 ReaderGroupConfig readerGroupConfig, String scope, String readerGroupName,
                                 DeserializationSchema<T> deserializationSchema,
                                 Time eventReadTimeout, Time checkpointInitiateTimeout,
                                 boolean enableMetrics) {
        this.clientConfig = Preconditions.checkNotNull(clientConfig, "clientConfig");
        this.readerGroupConfig = Preconditions.checkNotNull(readerGroupConfig, "readerGroupConfig");
        this.scope = Preconditions.checkNotNull(scope, "scope");
        this.readerGroupName = Preconditions.checkNotNull(readerGroupName, "readerGroupName");
        this.deserializationSchema = Preconditions.checkNotNull(deserializationSchema, "deserializationSchema");
        this.eventReadTimeout = Preconditions.checkNotNull(eventReadTimeout, "eventReadTimeout");
        this.checkpointInitiateTimeout = Preconditions.checkNotNull(checkpointInitiateTimeout, "checkpointInitiateTimeout");
        this.enableMetrics = enableMetrics;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, PravegaSplit> createReader(SourceReaderContext readerContext) {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<EventRead<T>>> elementsQueue =
                new FutureCompletingBlockingQueue<>();

        return new PravegaSourceReader<T>(
                elementsQueue,
                clientConfig,
                scope,
                readerGroupName,
                deserializationSchema,
                eventReadTimeout,
                new PravegaRecordEmitter<>(),
                new Configuration(),
                readerContext);
    }

    @Override
    public SplitEnumerator<PravegaSplit, Checkpoint> createEnumerator(
            SplitEnumeratorContext<PravegaSplit> enumContext) {
        return new PravegaSplitEnumerator(
                enumContext,
                this.scope,
                this.readerGroupName,
                this.clientConfig,
                this.readerGroupConfig,
                null);
    }

    @Override
    public SplitEnumerator<PravegaSplit, Checkpoint> restoreEnumerator(
            SplitEnumeratorContext<PravegaSplit> enumContext, Checkpoint checkpoint) throws IOException {
        log.info("Restore Enumerator called");
        return new PravegaSplitEnumerator(
                enumContext,
                this.scope,
                this.readerGroupName,
                this.clientConfig,
                this.readerGroupConfig,
                checkpoint);

    }

    @Override
    public SimpleVersionedSerializer<PravegaSplit> getSplitSerializer() {
        return new PravegaSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Checkpoint> getEnumeratorCheckpointSerializer() {
        return new CheckpointSerializer();
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return this.deserializationSchema.getProducedType();
    }

    // --------------- configurations -------------------------------
    /**
     * Gets a builder for {@link FlinkPravegaSource} to read Pravega streams using the Flink streaming API.
     * @param <T> the element type.
     */
    public static <T> FlinkPravegaSource.Builder<T> builder() {
        return new FlinkPravegaSource.Builder<>();
    }

    /**
     * A builder for {@link FlinkPravegaSource}.
     *
     * @param <T> the element type.
     */
    public static class Builder<T> extends AbstractStreamingReaderBuilder<T, FlinkPravegaSource.Builder<T>> {

        private DeserializationSchema<T> deserializationSchema;
        private SerializedValue<AssignerWithTimeWindows<T>> assignerWithTimeWindows;

        protected FlinkPravegaSource.Builder<T> builder() {
            return this;
        }

        /**
         * Sets the deserialization schema.
         *
         * @param deserializationSchema The deserialization schema
         * @return Builder instance.
         */
        public FlinkPravegaSource.Builder<T> withDeserializationSchema(DeserializationSchema<T> deserializationSchema) {
            this.deserializationSchema = deserializationSchema;
            return builder();
        }

        /**
         * Sets the timestamp and watermark assigner.
         *
         * @param assignerWithTimeWindows The timestamp and watermark assigner.
         * @return Builder instance.
         */

        public FlinkPravegaSource.Builder<T> withTimestampAssigner(AssignerWithTimeWindows<T> assignerWithTimeWindows) {
            try {
                ClosureCleaner.clean(assignerWithTimeWindows, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
                this.assignerWithTimeWindows = new SerializedValue<>(assignerWithTimeWindows);
            } catch (IOException e) {
                throw new IllegalArgumentException("The given assigner is not serializable", e);
            }
            return this;
        }

        @Override
        protected DeserializationSchema<T> getDeserializationSchema() {
            Preconditions.checkState(deserializationSchema != null, "Deserialization schema must not be null.");
            return deserializationSchema;
        }

        @Override
        protected SerializedValue<AssignerWithTimeWindows<T>> getAssignerWithTimeWindows() {
            return assignerWithTimeWindows;
        }

        /**
         * Builds a {@link FlinkPravegaReader} based on the configuration.
         * @throws IllegalStateException if the configuration is invalid.
         */
        public FlinkPravegaSource<T> build() {
            FlinkPravegaSource<T> source = buildSource();
            return source;
        }
    }
}
