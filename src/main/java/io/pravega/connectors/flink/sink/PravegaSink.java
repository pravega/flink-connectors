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
import io.pravega.connectors.flink.AbstractStreamingWriterBuilder;
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.connectors.flink.PravegaWriterMode;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Flink sink implementation for writing into pravega storage.
 * The flink pravega writer instance which can be added as a sink to a Flink job.
 *
 * @param <T>
 */
public class PravegaSink<T> implements Sink<T, PravegaTransactionState, Void, Void> {

    // flag to enable/disable metrics
    private final boolean enableMetrics;

    // The Pravega client config.
    private final ClientConfig clientConfig;

    private final Stream stream;

    // Various timeouts
    private final long txnLeaseRenewalPeriod;

    // The sink's mode of operation. This is used to provide different guarantees for the written events.
    private final PravegaWriterMode writerMode;

    private final SerializationSchema<T> serializationSchema;

    // The router used to partition events within a stream, can be null for random routing
    @Nullable
    private final PravegaEventRouter<T> eventRouter;

    public PravegaSink(boolean enableMetrics, ClientConfig clientConfig,
                       Stream stream, long txnLeaseRenewalPeriod, PravegaWriterMode writerMode,
                       SerializationSchema<T> serializationSchema, PravegaEventRouter<T> eventRouter) {
        this.enableMetrics = enableMetrics;
        this.clientConfig = clientConfig;
        this.stream = stream;
        this.txnLeaseRenewalPeriod = txnLeaseRenewalPeriod;
        this.writerMode = writerMode;
        this.serializationSchema = serializationSchema;
        this.eventRouter = eventRouter;
    }

    @Override
    public SinkWriter<T, PravegaTransactionState, Void> createWriter(
            InitContext context, List<Void> states) throws IOException {
        return new PravegaWriter<>(
                context,
                enableMetrics,
                clientConfig,
                stream,
                txnLeaseRenewalPeriod,
                writerMode,
                serializationSchema,
                eventRouter);
    }

    @Override
    public Optional<Committer<PravegaTransactionState>> createCommitter() throws IOException {
        return Optional.of(new PravegaCommitter<>(clientConfig, stream,
                txnLeaseRenewalPeriod, writerMode, serializationSchema, eventRouter));
    }

    @Override
    public Optional<GlobalCommitter<PravegaTransactionState, Void>> createGlobalCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<PravegaTransactionState>> getCommittableSerializer() {
        return Optional.of(new PravegaTransactionStateSerializer());
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getWriterStateSerializer() {
        return Optional.empty();
    }


    // ------------------------------------------------------------------------
    //  builder
    // ------------------------------------------------------------------------

    public static <T> PravegaSink.Builder<T> builder() {
        return new PravegaSink.Builder<>();
    }

    /**
     * A builder for {@link PravegaSink}.
     *
     * @param <T> the element type.
     */
    public static class Builder<T> extends AbstractStreamingWriterBuilder<T, Builder<T>> {

        private SerializationSchema<T> serializationSchema;

        @Nullable
        private PravegaEventRouter<T> eventRouter;

        protected Builder<T> builder() {
            return this;
        }

        /**
         * Sets the serialization schema.
         *
         * @param serializationSchema The serialization schema
         * @return Builder instance.
         */
        public Builder<T> withSerializationSchema(SerializationSchema<T> serializationSchema) {
            this.serializationSchema = serializationSchema;
            return builder();
        }

        /**
         * Sets the event router.
         *
         * @param eventRouter the event router which produces a key per event.
         * @return Builder instance.
         */
        public Builder<T> withEventRouter(PravegaEventRouter<T> eventRouter) {
            this.eventRouter = eventRouter;
            return builder();
        }

        /**
         * Builds the {@link PravegaSink}.
         *
         * @return An instance of {@link PravegaSink}
         */
        public PravegaSink<T> build() {
            Preconditions.checkState(serializationSchema != null, "Serialization schema must be supplied.");
            return createSink(serializationSchema, eventRouter);
        }
    }
}
