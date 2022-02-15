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

import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.connectors.flink.PravegaWriterMode;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

/**
 * A builder for {@link PravegaEventSink} and {@link PravegaTransactionSink}.
 *
 * @param <T> the element type.
 */
public class PravegaSinkBuilder<T> {
    // the numbers below are picked based on the default max settings in Pravega
    protected static final long DEFAULT_TXN_LEASE_RENEWAL_PERIOD_MILLIS = 600000; // 600 seconds

    private PravegaConfig pravegaConfig = PravegaConfig.fromDefaults();
    private String stream;
    private boolean enableMetrics = true;
    private PravegaWriterMode writerMode = PravegaWriterMode.ATLEAST_ONCE;
    private Time txnLeaseRenewalPeriod = Time.milliseconds(DEFAULT_TXN_LEASE_RENEWAL_PERIOD_MILLIS);
    private SerializationSchema<T> serializationSchema;
    @Nullable
    private PravegaEventRouter<T> eventRouter;

    PravegaSinkBuilder() {
    }

    static <T> PravegaSinkBuilder<T> builder() {
        return new PravegaSinkBuilder<>();
    }

    /**
     * Set the Pravega client configuration, which includes connection info, security info, and a default scope.
     * <p>
     * The default client configuration is obtained from {@code PravegaConfig.fromDefaults()}.
     *
     * @param pravegaConfig the configuration to use.
     * @return A builder to configure and create a sink.
     */
    public PravegaSinkBuilder<T> withPravegaConfig(PravegaConfig pravegaConfig) {
        this.pravegaConfig = pravegaConfig;
        return this;
    }

    /**
     * Add a stream to be written to by the sink.
     *
     * @param streamSpec the unqualified or qualified name of the stream.
     * @return A builder to configure and create a sink.
     */
    public PravegaSinkBuilder<T> forStream(final String streamSpec) {
        this.stream = streamSpec;
        return this;
    }

    /**
     * Add a stream to be written to by the sink.
     *
     * @param stream the stream.
     * @return A builder to configure and create a sink.
     */
    public PravegaSinkBuilder<T> forStream(final Stream stream) {
        this.stream = stream.getScopedName();
        return this;
    }

    /**
     * Enable/disable pravega sink metrics (default: enabled).
     *
     * @param enable boolean
     * @return A builder to configure and create a sink.
     */
    public PravegaSinkBuilder<T> enableMetrics(boolean enable) {
        this.enableMetrics = enable;
        return this;
    }

    /**
     * Sets the writer mode to provide at-least-once or exactly-once guarantees.
     *
     * @param writerMode The writer mode of {@code BEST_EFFORT}, {@code ATLEAST_ONCE}, or {@code EXACTLY_ONCE}.
     * @return A builder to configure and create a sink.
     */
    public PravegaSinkBuilder<T> withWriterMode(PravegaWriterMode writerMode) {
        this.writerMode = writerMode;
        return this;
    }

    /**
     * Sets the transaction lease renewal period.
     *
     * When the writer mode is set to {@code EXACTLY_ONCE}, transactions are used to persist
     * events to the Pravega stream. The transaction interval corresponds to the Flink checkpoint interval.
     * Throughout that interval, the transaction is kept alive with a lease that is periodically renewed.
     * This configuration setting sets the lease renewal period. The default value is 30 seconds.
     *
     * @param period the lease renewal period
     * @return A builder to configure and create a sink.
     */
    public PravegaSinkBuilder<T> withTxnLeaseRenewalPeriod(Time period) {
        Preconditions.checkArgument(period.getSize() > 0, "The timeout must be a positive value.");
        this.txnLeaseRenewalPeriod = period;
        return this;
    }

    /**
     * Sets the serialization schema.
     *
     * @param serializationSchema the deserialization schema to use.
     * @return A builder to configure and create a sink.
     */
    public PravegaSinkBuilder<T> withSerializationSchema(SerializationSchema<T> serializationSchema) {
        this.serializationSchema = serializationSchema;
        return this;
    }

    /**
     * Sets the event router.
     *
     * @param eventRouter the event router which produces a key per event.
     * @return A builder to configure and create a sink.
     */
    public PravegaSinkBuilder<T> withEventRouter(PravegaEventRouter<T> eventRouter) {
        this.eventRouter = eventRouter;
        return this;
    }

    /**
     * Resolves the stream to be provided to the sink, based on the configured default scope.
     *
     * @return the resolved stream instance.
     */
    public Stream resolveStream() {
        Preconditions.checkState(stream != null, "A stream must be supplied.");
        return pravegaConfig.resolve(stream);
    }

    /**
     * Create the EXACTLY_ONCE sink for the current builder state.
     *
     * @return An instance of {@link PravegaTransactionSink}.
     */
    public PravegaTransactionSink<T> buildTransactionSink() {
        Preconditions.checkState(writerMode == PravegaWriterMode.EXACTLY_ONCE,
                "writerMode must be EXACTLY_ONCE.");
        return new PravegaTransactionSink<>(
                enableMetrics,
                pravegaConfig.getClientConfig(),
                resolveStream(),
                txnLeaseRenewalPeriod.toMilliseconds(),
                serializationSchema,
                eventRouter);
    }

    /**
     * Create the BEST_EFFORT or ATLEAST_ONCE sink for the current builder state.
     *
     * @return An instance of {@link PravegaEventSink}.
     */
    public PravegaEventSink<T> buildEventSink() {
        Preconditions.checkState(writerMode == PravegaWriterMode.ATLEAST_ONCE || writerMode == PravegaWriterMode.BEST_EFFORT,
                "writerMode must be EXACTLY_ONCE.");
        return new PravegaEventSink<>(
                enableMetrics,
                pravegaConfig.getClientConfig(),
                resolveStream(),
                writerMode,
                serializationSchema,
                eventRouter);
    }
}
