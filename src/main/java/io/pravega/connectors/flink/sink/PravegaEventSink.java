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

import io.pravega.client.ClientConfig;
import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.PravegaEventRouter;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * A Pravega sink for {@link DeliveryGuarantee#NONE} and {@link DeliveryGuarantee#AT_LEAST_ONCE} writer mode.
 *
 * <p>Use {@link PravegaSinkBuilder} to construct a {@link PravegaEventSink}.
 *
 * <p>{@link PravegaEventWriter} spawned by this sink is the only class responsible for writing all incoming events.
 * In short, it calls {@link io.pravega.client.stream.EventStreamWriter#writeEvent} and fails the task if there is an error.
 *
 * @param <T> The type of the event to be written.
 */
@Experimental
public class PravegaEventSink<T> extends PravegaSink<T> {
    // The sink's mode of operation. This is used to provide different guarantees for the written events.
    private final DeliveryGuarantee writerMode;

    /**
     * Creates a new Pravega Event Sink instance which can be added as a sink to a Flink job.
     * It will create a {@link PravegaEventWriter} on demand with following parameters.
     * We can use {@link PravegaSinkBuilder} to build such a sink.
     *
     * @param clientConfig          The Pravega client configuration.
     * @param stream                The destination stream.
     * @param writerMode            The writer mode of the sink.
     * @param serializationSchema   The implementation for serializing every event into pravega's storage format.
     * @param eventRouter           The implementation to extract the partition key from the event.
     */
    public PravegaEventSink(ClientConfig clientConfig,
                            Stream stream, DeliveryGuarantee writerMode,
                            SerializationSchema<T> serializationSchema, PravegaEventRouter<T> eventRouter) {
        super(clientConfig, stream, serializationSchema, eventRouter);
        this.writerMode = Preconditions.checkNotNull(writerMode, "writerMode");
    }

    @Override
    public SinkWriter<T> createWriter(InitContext context) throws IOException {
        return new PravegaEventWriter<>(
                context,
                clientConfig,
                stream,
                writerMode,
                serializationSchema,
                eventRouter);
    }
}
