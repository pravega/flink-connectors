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

import io.pravega.client.stream.impl.JavaSerializer;
import java.io.Serializable;

/**
 * Helper methods to create DeserializationSchema and SerializationSchemas using the pravega JavaSerializer
 * for generic types.
 */
public class PravegaSerialization {
    public static final <T extends Serializable> PravegaDeserializationSchema<T> deserializationFor(Class<T> type) {
        return new PravegaDeserializationSchema<>(type, new JavaSerializer<>());
    }

    public static final <T extends Serializable> PravegaSerializationSchema<T> serializationFor(Class<T> type) {
        return new PravegaSerializationSchema<>(new JavaSerializer<T>());
    }
}
