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

import io.pravega.client.stream.EventRead;
import io.pravega.connectors.flink.FlinkPravegaReader;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A Pravega DeserializationSchema that enables deserializing events together with
 * the Pravega {@link EventRead} metadata, this can be used for recording and indexing use cases. <p>
 *
 * This deserialization schema disables the
 * {@link PravegaDeserializationSchemaWithMetadata#deserialize(byte[])} method and
 * delegates the deserialization to
 * {@link PravegaDeserializationSchemaWithMetadata#deserialize(byte[], EventRead)}.
 * {@link FlinkPravegaReader} will distinguish this from a normal deserialization schema and
 * call {@link PravegaDeserializationSchemaWithMetadata#deserialize(byte[], EventRead)} when it is reading events.
 */
public abstract class PravegaDeserializationSchemaWithMetadata<T> implements DeserializationSchema<T> {
    public abstract T deserialize(byte[] message, EventRead<ByteBuffer> eventRead) throws IOException;

    public void deserialize(byte[] message, EventRead<ByteBuffer> eventRead, Collector<T> out) throws IOException {
        T deserialize = deserialize(message, eventRead);
        if (deserialize != null) {
            out.collect(deserialize);
        }
    }

    public T deserialize(byte[] message) throws IOException {
        throw new IllegalStateException("Should never be called.");
    }
}
