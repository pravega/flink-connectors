/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.connectors.flink.serialization;

import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.connectors.flink.PravegaCollector;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class PravegaSerializationTest {

    @Test
    public void testSerialization() throws IOException {
        PravegaSerializationSchema<String> serializer = new PravegaSerializationSchema<>(new JavaSerializer<>());
        PravegaDeserializationSchema<String> deserializer = new PravegaDeserializationSchema<>(String.class, new JavaSerializer<>());

        String input = "Testing input";
        byte[] serialized = serializer.serialize(input);
        assertThat(deserializer.deserialize(serialized)).isEqualTo(input);
    }

    @Test
    public void testCollectorSerialization() throws IOException {
        PravegaSerializationSchema<String> serializer = new PravegaSerializationSchema<>(new JavaSerializer<>());
        PravegaDeserializationSchema<String> deserializer = new PravegaDeserializationSchema<>(String.class, new JavaSerializer<>());

        String input = "Testing input";
        byte[] serialized = serializer.serialize(input);

        PravegaCollector<String> pravegaCollector = new PravegaCollector<>(deserializer);
        deserializer.deserialize(serialized, pravegaCollector);
        assertThat(pravegaCollector.getRecords().size()).isEqualTo(1);

        String deserialized = pravegaCollector.getRecords().poll();
        assertThat(deserialized).isEqualTo(input);
    }

    @Test
    public void testNotFullyWrappingByteBuffer() throws IOException {
        for (boolean direct : new boolean[] { true, false} ) {
            runNotFullyWrappingByteBufferTest(8, 0, 8, direct);
            runNotFullyWrappingByteBufferTest(32, 0, 32, direct);
            runNotFullyWrappingByteBufferTest(16, 2, 11, direct);
            runNotFullyWrappingByteBufferTest(24, 1, 19, direct);
            runNotFullyWrappingByteBufferTest(15, 3, 8, direct);
        }
        
    }

    private void runNotFullyWrappingByteBufferTest(int arraySize, int offset, int capacity, boolean direct) throws IOException {
        final Serializer<Long> pravegaSerializer =
                new ByteBufferReusingSerializer(arraySize, offset, capacity, direct);

        final SerializationSchema<Long> flinkSerializer = new PravegaSerializationSchema<>(pravegaSerializer);

        final Random rnd = new Random();

        for (int num = 100; num > 0; --num) {
            final long value = rnd.nextLong();

            byte[] serialized = flinkSerializer.serialize(value);
            assertThat(ByteBuffer.wrap(serialized).getLong()).isEqualTo(value);
        }
    }

    @Test
    public void testSerializerFastPath() throws IOException {
        final FastSerializer pravegaSerializer = new FastSerializer();
        final SerializationSchema<Long> flinkSerializer = new PravegaSerializationSchema<>(pravegaSerializer);

        final Random rnd = new Random();

        for (int num = 100; num > 0; --num) {
            final long value = rnd.nextLong();

            byte[] serialized = flinkSerializer.serialize(value);
            assertThat(ByteBuffer.wrap(serialized).getLong()).isEqualTo(value);

            // make sure we avoid copies where possible
            assertThat(serialized == pravegaSerializer.array).isTrue();
        }
    }

    // ------------------------------------------------------------------------

    /**
     * A test Pravega Serializer that uses a ByteBuffer that may be too large or a
     * using only a slice of it's backing array
     */
    private static class ByteBufferReusingSerializer implements Serializer<Long> {

        private final ByteBuffer buffer;

        private ByteBufferReusingSerializer(int capacity, int offset, int size, boolean direct) {
            // we create some sliced byte buffers that do not always the first
            // bytes or all bytes of the backing array;
            final ByteBuffer buffer = direct ? ByteBuffer.allocateDirect(capacity) :
                    ByteBuffer.allocate(capacity);

            if (size == capacity && offset == 0) {
                this.buffer = buffer;
            } else {
                buffer.position(offset);
                buffer.limit(offset + size);
                this.buffer = buffer.slice();
            }
        }

        @Override
        public ByteBuffer serialize(Long value) {
            buffer.clear();
            buffer.putLong(value);
            buffer.flip();
            return buffer;
        }

        @Override
        public Long deserialize(ByteBuffer byteBuffer) {
            return byteBuffer.getLong();
        }
    }

    // ------------------------------------------------------------------------

    private static class FastSerializer implements Serializer<Long> {

        final byte[] array = new byte[8];

        @Override
        public ByteBuffer serialize(Long value) {
            ByteBuffer buf = ByteBuffer.wrap(array);
            buf.putLong(value);
            buf.flip();
            return buf;
        }

        @Override
        public Long deserialize(ByteBuffer byteBuffer) {
            return byteBuffer.getLong();
        }
    }

    @Test
    public void testJsonSerializer() throws IOException {
        final JsonSerializer<TestEvent> jsonSerializer = new JsonSerializer<>(TestEvent.class);
        TestEvent testEvent = new TestEvent("key1", 1);
        ByteBuffer serializedBytes = jsonSerializer.serialize(testEvent);
        assertThat(jsonSerializer.deserialize(serializedBytes)).isEqualTo(testEvent);
    }

    // ------------------------------------------------------------------------

    private static class TestEvent implements Serializable {
        private String key;
        private int value;
        public TestEvent() {}

        public TestEvent(String key, int value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public int getValue() {
            return value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestEvent testEvent = (TestEvent) o;
            return key.equals(testEvent.key) &&
                    value == testEvent.value;
        }
    }
}
