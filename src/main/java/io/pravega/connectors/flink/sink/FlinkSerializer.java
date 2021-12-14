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
package io.pravega.connectors.flink.sink;

import io.pravega.client.stream.Serializer;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.nio.ByteBuffer;

/**
 * Wrap the {@link SerializationSchema} to a Pravega compatible {@link Serializer}.
 *
 * @param <T> The type of the event.
 */
@VisibleForTesting
class FlinkSerializer<T> implements Serializer<T> {

    private final SerializationSchema<T> serializationSchema;

    FlinkSerializer(SerializationSchema<T> serializationSchema) {
        this.serializationSchema = serializationSchema;
    }

    @Override
    public ByteBuffer serialize(T value) {
        return ByteBuffer.wrap(serializationSchema.serialize(value));
    }

    @Override
    public T deserialize(ByteBuffer serializedValue) {
        throw new IllegalStateException("deserialize() called within a serializer");
    }
}
