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

import io.pravega.client.ClientFactory;
import io.pravega.client.batch.BatchClient;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.serialization.WrappingSerializer;
import io.pravega.connectors.flink.util.FlinkPravegaUtils;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.core.io.InputSplitAssigner;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * A Flink {@link InputFormat} that can be added as a source to read from Pravega in a Flink batch job.
 */
@Slf4j
public class FlinkPravegaInputFormat<T> extends RichInputFormat<T, PravegaInputSplit> {

    private static final long serialVersionUID = 1L;

    // The supplied event deserializer.
    private final DeserializationSchema<T> deserializationSchema;

    // The pravega controller endpoint.
    private final URI controllerURI;

    // The scope name of the destination stream.
    private final String scopeName;

    // The names of Pravega streams to read.
    private final Set<String> streamNames;

    // The factory used to create Pravega clients; closing this will also close all Pravega connections.
    private transient ClientFactory clientFactory;

    // The batch client used to read Pravega segments; this client is reused for all segments read by this input format.
    private transient BatchClient batchClient;

    // The iterator for the currently read input split (i.e. a Pravega segment).
    private transient SegmentIterator<T> segmentIterator;

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
     * @param deserializationSchema The implementation to deserialize events from pravega streams.
     */
    public FlinkPravegaInputFormat(
            final URI controllerURI,
            final String scope,
            final Set<String> streamNames,
            final DeserializationSchema<T> deserializationSchema) {

        Preconditions.checkNotNull(controllerURI, "controllerURI");
        Preconditions.checkNotNull(scope, "scope");
        Preconditions.checkNotNull(streamNames, "streamNames");
        Preconditions.checkNotNull(deserializationSchema, "deserializationSchema");

        this.controllerURI = controllerURI;
        this.scopeName = scope;
        this.deserializationSchema = deserializationSchema;
        this.streamNames = streamNames;
    }

    // ------------------------------------------------------------------------
    //  Input format life cycle methods
    // ------------------------------------------------------------------------

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();

        this.clientFactory = ClientFactory.withScope(scopeName, controllerURI);
        this.batchClient = clientFactory.createBatchClient();
    }

    @Override
    public void closeInputFormat() throws IOException {
        // closing the client factory also closes the batch client connection
        this.clientFactory.close();
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
        try (ClientFactory clientFactory = ClientFactory.withScope(scopeName, controllerURI)) {
            BatchClient batchClient = clientFactory.createBatchClient();

            for (String stream : streamNames) {

                Iterator<SegmentRange> segmentRangeIterator =
                        batchClient.getSegments(Stream.of(scopeName, stream), null, null).getIterator();
                while (segmentRangeIterator.hasNext()) {
                    splits.add(new PravegaInputSplit(splits.size(),
                            segmentRangeIterator.next()));
                }
            }
        }

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
        this.segmentIterator = batchClient.readSegment(split.getSegmentRange(), deserializer);
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
}
