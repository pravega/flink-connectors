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

package org.apache.flink.streaming.connectors.pravega.util;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.Serializer;
import org.apache.flink.streaming.connectors.pravega.EventTimeOrderingOperator;
import org.apache.flink.streaming.connectors.pravega.FlinkPravegaWriter;
import org.apache.flink.streaming.connectors.pravega.serialization.WrappingSerializer;
import lombok.SneakyThrows;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.api.common.serialization.DeserializationSchema;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.stream.Collectors;
public class FlinkPravegaUtils {

    private FlinkPravegaUtils() {
    }

    /**
     * Writes a stream of elements to a Pravega stream with event time ordering.
     * <p>
     * This method returns a sink that automatically reorders events by their event timestamp.  The ordering
     * is preserved by Pravega using the associated routing key.
     *
     * @param stream      the stream to read.
     * @param writer      the Pravega writer to use.
     * @param parallelism the degree of parallelism for the writer.
     * @param <T>         The type of the event.
     * @return a sink.
     */
    public static <T> DataStreamSink<T> writeToPravegaInEventTimeOrder(DataStream<T> stream, FlinkPravegaWriter<T> writer, int parallelism) {
        // a keyed stream is used to ensure that all elements for a given key are forwarded to the same writer instance.
        // the parallelism must match between the ordering operator and the sink operator to ensure that
        // a forwarding strategy (as opposed to a rebalancing strategy) is used by Flink between the two operators.
        return stream
                .keyBy(new PravegaEventRouterKeySelector<>(writer.getEventRouter()))
                .transform("reorder", stream.getType(), new EventTimeOrderingOperator<>()).setParallelism(parallelism).forward()
                .addSink(writer).setParallelism(parallelism);
    }

    /**
     * Utility method that derives default reader name from stream and scope name.
     *
     * @param scope The destination streams' scope name.
     * @param streamNames Set of stream to read, used to generate the reader name.
     * @return the generated default reader name.
     */
    public static String getDefaultReaderName(final String scope, final Set<String> streamNames) {
        final String delimiter = "-";
        final String reader = streamNames.stream().collect(Collectors.joining(delimiter)) + delimiter + scope;
        int hash = 0;
        for (int i = 0; i < reader.length(); i++) {
            hash = reader.charAt(i) + (31 * hash);
        }
        return Integer.toString(hash);
    }

    /**
     * Generates a random reader group name.
     *
     * @return generated reader group name.
     */
    public static String generateRandomReaderGroupName() {
        return "flink" + RandomStringUtils.randomAlphanumeric(20).toLowerCase();
    }

    /**
     * Creates a Pravga {@link EventStreamReader}.
     *
     * @param clientConfig The Pravega client configuration.
     * @param readerId The id of the Pravega reader.
     * @param readerGroupScopeName The reader group scope name.
     * @param readerGroupName The reader group name.
     * @param deserializationSchema The implementation to deserialize events from pravega streams.
     * @param readerConfig The reader configuration.
     * @param <T> The type of the event.
     * @return the create Pravega reader.
     */
    public static <T> EventStreamReader<T> createPravegaReader(
            ClientConfig clientConfig,
            String readerId,
            String readerGroupScopeName,
            String readerGroupName,
            DeserializationSchema<T> deserializationSchema,
            ReaderConfig readerConfig) {

        // create the adapter between Pravega's serializers and Flink's serializers
        @SuppressWarnings("unchecked")
        final Serializer<T> deserializer = deserializationSchema instanceof WrappingSerializer
                ? ((WrappingSerializer<T>) deserializationSchema).getWrappedSerializer()
                : new FlinkDeserializer<>(deserializationSchema);

        return EventStreamClientFactory.withScope(readerGroupScopeName, clientConfig)
                .createReader(readerId, readerGroupName, deserializer, readerConfig);
    }

    /**
     * A Pravega {@link Serializer} that wraps around a Flink {@link DeserializationSchema}.
     *
     * @param <T> The type of the event.
     */
    public static final class FlinkDeserializer<T> implements Serializer<T> {

        private final DeserializationSchema<T> deserializationSchema;

        public FlinkDeserializer(DeserializationSchema<T> deserializationSchema) {
            this.deserializationSchema = deserializationSchema;
        }

        @Override
        public ByteBuffer serialize(T value) {
            throw new IllegalStateException("serialize() called within a deserializer");
        }

        @Override
        @SneakyThrows
        public T deserialize(ByteBuffer buffer) {
            byte[] array;
            if (buffer.hasArray() && buffer.arrayOffset() == 0 && buffer.position() == 0 && buffer.limit() == buffer.capacity()) {
                array = buffer.array();
            } else {
                array = new byte[buffer.remaining()];
                buffer.get(array);
            }

            return deserializationSchema.deserialize(array);
        }
    }
}
