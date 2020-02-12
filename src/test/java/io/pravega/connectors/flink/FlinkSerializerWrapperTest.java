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

import io.pravega.client.stream.Serializer;

import io.pravega.connectors.flink.util.FlinkPravegaUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class FlinkSerializerWrapperTest {

    @Test
    public void testNotFullyWrappingByteBuffer() throws IOException {
        for (boolean direct : new boolean[] { false, true} ) {
            runBufferLargerThanEventTest(8, 0, 8, direct);
            runBufferLargerThanEventTest(32, 0, 32, direct);
            runBufferLargerThanEventTest(16, 2, 11, direct);
            runBufferLargerThanEventTest(24, 1, 19, direct);
            runBufferLargerThanEventTest(15, 3, 8, direct);
        }

    }

    private void runBufferLargerThanEventTest(int capacity, int offset, int size, boolean direct) throws IOException {
        final DeserializationSchema<Long> flinkDeserializer = new LongDeserializationSchema();
        final Serializer<Long> wrappingSerializer = new FlinkPravegaUtils.FlinkDeserializer<>(flinkDeserializer);

        // we create some sliced byte buffers that do not always the first
        // bytes or all bytes of the backing array;
        final ByteBuffer rawBuffer = direct ? ByteBuffer.allocateDirect(capacity) :
                ByteBuffer.allocate(capacity);
        final ByteBuffer buffer;

        if (size == capacity && offset == 0) {
            buffer = rawBuffer;
        } else {
            rawBuffer.position(offset);
            rawBuffer.limit(offset + size);
            buffer = rawBuffer.slice();
        }
        
        final Random rnd = new Random();

        for (int num = 100; num > 0; --num) {
            final long value = rnd.nextLong();
            buffer.clear();
            buffer.putLong(value);
            buffer.flip();

            long deserialized = wrappingSerializer.deserialize(buffer);
            assertEquals(value, deserialized);
        }
    }

    // ------------------------------------------------------------------------

    private static class LongDeserializationSchema implements DeserializationSchema<Long> {

        @Override
        public Long deserialize(byte[] message) throws IOException {
            return ByteBuffer.wrap(message).getLong();
        }

        @Override
        public boolean isEndOfStream(Long nextElement) {
            return false;
        }

        @Override
        public TypeInformation<Long> getProducedType() {
            // not relevant for this test
            throw new UnsupportedOperationException("not implemented");
        }
    }
}
