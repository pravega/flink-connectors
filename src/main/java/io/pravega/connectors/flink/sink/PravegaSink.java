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
import io.pravega.connectors.flink.PravegaWriterMode;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

/**
 * Pravega Sink writes data into a Pravega stream. It supports all writer mode
 * described by {@link PravegaWriterMode}.
 *
 * <p>For {@link PravegaWriterMode#ATLEAST_ONCE} and {@link PravegaWriterMode#ATLEAST_ONCE},
 * a {@link PravegaEventSink} will be returned after {@link PravegaSinkBuilder#build()}.
 *
 * <p>For {@link PravegaWriterMode#EXACTLY_ONCE}, a {@link PravegaTransactionalSink}
 * will be returned after {@link PravegaSinkBuilder#build()}.
 *
 * @param <T> The type of the event to be written.
 */
@Experimental
public abstract class PravegaSink<T> implements Sink<T> {
    static final String PRAVEGA_WRITER_METRICS_GROUP = "PravegaWriter";
    static final String SCOPED_STREAM_METRICS_GAUGE = "stream";

    // flag to enable/disable metrics
    final boolean enableMetrics;

    // The Pravega client config.
    final ClientConfig clientConfig;

    // The destination stream.
    final Stream stream;

    // The supplied event serializer.
    final SerializationSchema<T> serializationSchema;

    // The router used to partition events within a stream, can be null for random routing
    @Nullable
    final PravegaEventRouter<T> eventRouter;

    /**
     * Set common parameters for {@link PravegaEventSink} and {@link PravegaTransactionalSink}.
     *
     * @param enableMetrics         Flag to indicate whether metrics needs to be enabled or not.
     * @param clientConfig          The Pravega client configuration.
     * @param stream                The destination stream.
     * @param serializationSchema   The implementation for serializing every event into pravega's storage format.
     * @param eventRouter           The implementation to extract the partition key from the event.
     */
    PravegaSink(boolean enableMetrics, ClientConfig clientConfig, Stream stream,
                SerializationSchema<T> serializationSchema, PravegaEventRouter<T> eventRouter) {
        this.enableMetrics = enableMetrics;
        this.clientConfig = Preconditions.checkNotNull(clientConfig, "clientConfig");
        this.stream = Preconditions.checkNotNull(stream, "stream");
        this.serializationSchema = Preconditions.checkNotNull(serializationSchema, "serializationSchema");
        this.eventRouter = eventRouter;
    }

    /**
     * A helper method to register metrics is enabled.
     *
     * @param enableMetrics         Flag to indicate whether metrics needs to be enabled or not.
     * @param context               The interface exposes some runtime info for creating a {@link SinkWriter}.
     */
    void registerMetrics(boolean enableMetrics, InitContext context) {
        if (enableMetrics) {
            MetricGroup pravegaWriterMetricGroup = context.metricGroup().addGroup(PRAVEGA_WRITER_METRICS_GROUP);
            pravegaWriterMetricGroup.gauge(SCOPED_STREAM_METRICS_GAUGE, new StreamNameGauge(stream.getScopedName()));
        }
    }
}
