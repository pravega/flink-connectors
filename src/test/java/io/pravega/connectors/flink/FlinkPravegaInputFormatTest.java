/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink;


import io.pravega.client.BatchClientFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.batch.StreamSegmentsIterator;
import io.pravega.client.stream.Stream;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.io.InputFormat;
import org.junit.Test;

import java.util.Iterator;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Unit Test for the {@link FlinkPravegaInputFormat}
 */
public class FlinkPravegaInputFormatTest {

    private final DeserializationSchema<String> deserializationSchema = mock(DeserializationSchema.class);

    private BatchClientFactory batchClientFactory = mock(BatchClientFactory.class);

    private SegmentIterator<String> segmentIterator = mock(SegmentIterator.class);

    private Iterator<SegmentRange> segmentRangeIterator = mock(Iterator.class);

    private StreamSegmentsIterator streamSegmentsIterator = mock(StreamSegmentsIterator.class);

    private final PravegaConfig pravegaConfig = mock(PravegaConfig.class);

    private final Stream stream = Stream.of("test", "foo");

    private final ClientConfig clientConfig = ClientConfig.builder().build();

    /**
     * Testing the builder for right configurations.
     */
    @Test
    public void testBuilderForSuccess() {
        doReturn(clientConfig).when(pravegaConfig).getClientConfig();
        doReturn(stream).when(pravegaConfig).resolve(anyString());
        FlinkPravegaInputFormat.<String>builder()
                .withDeserializationSchema(deserializationSchema)
                .withPravegaConfig(pravegaConfig)
                .forStream(stream)
                .build();
    }

    /**
     * Testing the builder for missing configurations.
     */
    @Test (expected = IllegalStateException.class)
    public void testBuilderForMissingDeSerializationSchema() {
        doReturn(clientConfig).when(pravegaConfig).getClientConfig();
        doReturn(stream).when(pravegaConfig).resolve(anyString());
        FlinkPravegaInputFormat.<String>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(stream)
                .build();
    }

    /**
     * Testing the builder for missing configurations.
     */
    @Test (expected = IllegalStateException.class)
    public void testBuilderForMissingStream() {
        doReturn(clientConfig).when(pravegaConfig).getClientConfig();
        doReturn(stream).when(pravegaConfig).resolve(anyString());
        FlinkPravegaInputFormat.<String>builder()
                .withDeserializationSchema(deserializationSchema)
                .withPravegaConfig(pravegaConfig)
                .build();
    }

    /**
     * Test case to validate the lifecycle methods of {@link InputFormat}
     *
     */
    @Test
    public void testLifecycleMethods() throws Exception  {

        FlinkPravegaInputFormat spyFlinkPravegaInputFormat = spyFlinkPravegaInputFormat();

        spyFlinkPravegaInputFormat.openInputFormat();
        verify(spyFlinkPravegaInputFormat).openInputFormat();

        PravegaInputSplit pravegaInputSplit = mock(PravegaInputSplit.class);
        spyFlinkPravegaInputFormat.open(pravegaInputSplit);
        verify(spyFlinkPravegaInputFormat).open(pravegaInputSplit);

        PravegaInputSplit[] inputSplits = new PravegaInputSplit[0];
        spyFlinkPravegaInputFormat.getInputSplitAssigner(inputSplits);
        verify(spyFlinkPravegaInputFormat).getInputSplitAssigner(inputSplits);

        int minSplits = 2;
        spyFlinkPravegaInputFormat.createInputSplits(minSplits);
        verify(spyFlinkPravegaInputFormat).createInputSplits(minSplits);

        spyFlinkPravegaInputFormat.reachedEnd();
        verify(spyFlinkPravegaInputFormat).reachedEnd();
        verify(segmentIterator).hasNext();

        spyFlinkPravegaInputFormat.nextRecord("test");
        verify(segmentIterator).next();

        spyFlinkPravegaInputFormat.closeInputFormat();
        verify(spyFlinkPravegaInputFormat).closeInputFormat();

        spyFlinkPravegaInputFormat.close();
        verify(spyFlinkPravegaInputFormat).close();
        verify(segmentIterator).close();
    }

    private FlinkPravegaInputFormat<String> spyFlinkPravegaInputFormat() {
        doReturn(clientConfig).when(pravegaConfig).getClientConfig();
        doReturn(stream).when(pravegaConfig).resolve(anyString());
        FlinkPravegaInputFormat<String> flinkPravegaInputFormat = FlinkPravegaInputFormat.<String>builder()
                                                            .withDeserializationSchema(deserializationSchema)
                                                            .withPravegaConfig(pravegaConfig)
                                                            .forStream(stream)
                                                            .build();
        FlinkPravegaInputFormat<String> spyFlinkPravegaInputFormat = spy(flinkPravegaInputFormat);

        doReturn(batchClientFactory).when(spyFlinkPravegaInputFormat).getBatchClientFactory(anyString(), anyObject());
        doReturn(streamSegmentsIterator).when(batchClientFactory).getSegments(any(), any(), any());
        doReturn(segmentRangeIterator).when(streamSegmentsIterator).getIterator();
        doReturn(segmentIterator).when(batchClientFactory).readSegment(any(), any());
        doReturn(Boolean.TRUE).when(segmentIterator).hasNext();
        doReturn("foo").when(segmentIterator).next();

        DefaultInputSplitAssigner defaultInputSplitAssigner = mock(DefaultInputSplitAssigner.class);
        doReturn(defaultInputSplitAssigner).when(spyFlinkPravegaInputFormat).getInputSplitAssigner(any());
        return spyFlinkPravegaInputFormat;
    }
}
