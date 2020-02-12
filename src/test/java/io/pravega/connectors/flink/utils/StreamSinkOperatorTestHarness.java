/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink.utils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

/**
 * A test harness for {@link StreamSink StreamSink} operators and {@link SinkFunction SinkFunctions}.
 *
 * @param <T> The input type of the sink.
 */
public class StreamSinkOperatorTestHarness<T> extends OneInputStreamOperatorTestHarness<T, Object> implements AutoCloseable {

    public StreamSinkOperatorTestHarness(SinkFunction<T> function, TypeSerializer<T> typeSerializerIn) throws Exception {
        this(new StreamSink<>(function), typeSerializerIn);
    }

    public StreamSinkOperatorTestHarness(StreamSink<T> operator, TypeSerializer<T> typeSerializerIn) throws Exception {
        super(operator, typeSerializerIn);
    }
}
