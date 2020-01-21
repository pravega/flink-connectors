/**
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

package org.apache.flink.streaming.connectors.pravega.serialization;

import io.pravega.client.stream.Serializer;
import java.nio.ByteBuffer;
import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * A serialization schema adapter for a Pravega serializer.
 */
public class PravegaSerializationSchema<T> 
        implements SerializationSchema<T>, WrappingSerializer<T> {

    // the Pravega serializer
    private final Serializer<T> serializer;

    public PravegaSerializationSchema(Serializer<T> serializer) {
        this.serializer = serializer;
    }

    @Override
    public byte[] serialize(T element) {
        ByteBuffer buf = serializer.serialize(element);
        
        if (buf.hasArray() && buf.arrayOffset() == 0 && buf.position() == 0 && buf.limit() == buf.capacity()) {
            return buf.array();
        } else {
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);
            return bytes;
        }
    }

    @Override
    public Serializer<T> getWrappedSerializer() {
        return serializer;
    }
}
