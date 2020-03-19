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

import io.pravega.client.ClientConfig;
import io.pravega.client.BatchClientFactory;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.stream.Serializer;
import io.pravega.connectors.flink.serialization.WrappingSerializer;
import io.pravega.connectors.flink.util.FlinkPravegaUtils;

import io.pravega.connectors.flink.util.StreamWithBoundaries;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.core.io.InputSplitAssigner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A Flink {@link InputFormat} that can be added as a source to read from Pravega in a Flink batch job.
 */
@Slf4j
public class FlinkPravegaInputFormat<T> extends RichInputFormat<T, PravegaInputSplit> {

    private static final long serialVersionUID = 1L;

    private static final String DEFAULT_CLIENT_SCOPE_NAME = "__NOT_USED";

    // The Pravega client configuration.
    private final ClientConfig clientConfig;

    // The scope name to use to construct the Pravega client.
    private final String clientScope;

    // The names of Pravega streams to read.
    private final List<StreamWithBoundaries> streams;

    // The supplied event deserializer.
    private final DeserializationSchema<T> deserializationSchema;

    // The batch client factory implementation used to read Pravega segments; this instance is reused for all segments read by this input format.
    private transient BatchClientFactory batchClientFactory;

    // The iterator for the currently read input split (i.e. a Pravega segment).
    private transient SegmentIterator<T> segmentIterator;

    /**
     * Creates a new Flink Pravega {@link InputFormat} which can be added as a source to a Flink batch job.
     *
     * @param clientConfig          The pravega client configuration.
     * @param streams               The list of streams to read events from.
     * @param deserializationSchema The implementation to deserialize events from pravega streams.
     */
    protected FlinkPravegaInputFormat(
            ClientConfig clientConfig,
            List<StreamWithBoundaries> streams,
            DeserializationSchema<T> deserializationSchema) {
        this.clientConfig = Preconditions.checkNotNull(clientConfig, "clientConfig");
        this.clientScope = DEFAULT_CLIENT_SCOPE_NAME;
        this.streams = Preconditions.checkNotNull(streams, "streams");
        this.deserializationSchema = Preconditions.checkNotNull(deserializationSchema, "deserializationSchema");
    }

    // ------------------------------------------------------------------------
    //  Input format life cycle methods
    // ------------------------------------------------------------------------

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        this.batchClientFactory = getBatchClientFactory(clientScope, clientConfig);
    }

    @VisibleForTesting
    protected BatchClientFactory getBatchClientFactory(String clientScope, ClientConfig clientConfig) {
        return BatchClientFactory.withScope(clientScope, clientConfig);
    }

    @Override
    public void closeInputFormat() throws IOException {
        // closing the client factory also closes the batch client connection
        this.batchClientFactory.close();
    }

    @Override
    public void configure(Configuration parameters) {
        // nothing to configure
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        // no statistics available, by default.
        return cachedStatistics;
    }

    @Override
    public PravegaInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        List<PravegaInputSplit> splits = new ArrayList<>();

        // createInputSplits() is called in the JM, so we have to establish separate
        // short-living connections to Pravega here to retrieve the segments list
        try (
                BatchClientFactory batchClientFactory = getBatchClientFactory(clientScope, clientConfig)
            ) {

            for (StreamWithBoundaries stream : streams) {
                Iterator<SegmentRange> segmentRangeIterator =
                        batchClientFactory.getSegments(stream.getStream(), stream.getFrom(), stream.getTo()).getIterator();
                while (segmentRangeIterator.hasNext()) {
                    splits.add(new PravegaInputSplit(splits.size(), segmentRangeIterator.next()));
                }
            }
        }

        log.info("Prepared {} input splits", splits.size());
        return splits.toArray(new PravegaInputSplit[splits.size()]);
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(PravegaInputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    // ------------------------------------------------------------------------
    //  Input split life cycle methods
    // ------------------------------------------------------------------------

    @Override
    public void open(PravegaInputSplit split) throws IOException {
        // create the adapter between Pravega's serializers and Flink's serializers
        @SuppressWarnings("unchecked")
        final Serializer<T> deserializer = deserializationSchema instanceof WrappingSerializer
                ? ((WrappingSerializer<T>) deserializationSchema).getWrappedSerializer()
                : new FlinkPravegaUtils.FlinkDeserializer<>(deserializationSchema);

        // build a new iterator for each input split.  Note that the endOffset parameter is not used by the Batch API at the moment.
        this.segmentIterator = batchClientFactory.readSegment(split.getSegmentRange(), deserializer);
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !this.segmentIterator.hasNext();
    }

    @Override
    public T nextRecord(T t) throws IOException {
        return this.segmentIterator.next();
    }

    @Override
    public void close() throws IOException {
        this.segmentIterator.close();
    }

    /**
     * Gets a builder {@link FlinkPravegaInputFormat} to read Pravega streams using the Flink batch API.
     * @param <T> the element type.
     */
    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /**
     * A builder for {@link FlinkPravegaInputFormat} to read Pravega streams using the Flink batch API.
     *
     * @param <T> the element type.
     */
    public static class Builder<T> extends AbstractReaderBuilder<FlinkPravegaInputFormat.Builder<T>> {

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

        protected DeserializationSchema<T> getDeserializationSchema() {
            Preconditions.checkState(deserializationSchema != null, "Deserialization schema must not be null.");
            return deserializationSchema;
        }

        public FlinkPravegaInputFormat<T> build() {
            return new FlinkPravegaInputFormat<>(getPravegaConfig().getClientConfig(), resolveStreams(), getDeserializationSchema());
        }
    }
}
