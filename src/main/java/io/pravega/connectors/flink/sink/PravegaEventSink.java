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
import java.io.IOException;

@Experimental
public class PravegaEventSink<T> implements Sink<T> {
    private static final String PRAVEGA_WRITER_METRICS_GROUP = "PravegaWriter";
    private static final String SCOPED_STREAM_METRICS_GAUGE = "stream";

    // flag to enable/disable metrics
    private final boolean enableMetrics;

    // The Pravega client config.
    private final ClientConfig clientConfig;

    // The destination stream.
    private final Stream stream;

    // The sink's mode of operation. This is used to provide different guarantees for the written events.
    private final PravegaWriterMode writerMode;

    // The supplied event serializer.
    private final SerializationSchema<T> serializationSchema;

    // The router used to partition events within a stream, can be null for random routing
    @Nullable
    private final PravegaEventRouter<T> eventRouter;

    public PravegaEventSink(boolean enableMetrics, ClientConfig clientConfig,
                            Stream stream, PravegaWriterMode writerMode,
                            SerializationSchema<T> serializationSchema, PravegaEventRouter<T> eventRouter) {
        this.enableMetrics = enableMetrics;
        this.clientConfig = Preconditions.checkNotNull(clientConfig, "clientConfig");
        this.stream = Preconditions.checkNotNull(stream, "stream");
        this.writerMode = Preconditions.checkNotNull(writerMode, "writerMode");
        this.serializationSchema = Preconditions.checkNotNull(serializationSchema, "serializationSchema");
        this.eventRouter = eventRouter;
    }

    @Override
    public SinkWriter<T> createWriter(InitContext context) throws IOException {
        if (enableMetrics) {
            MetricGroup pravegaWriterMetricGroup = context.metricGroup().addGroup(PRAVEGA_WRITER_METRICS_GROUP);
            pravegaWriterMetricGroup.gauge(SCOPED_STREAM_METRICS_GAUGE, new StreamNameGauge(stream.getScopedName()));
        }

        return new PravegaEventWriter<>(
                context,
                clientConfig,
                stream,
                writerMode,
                serializationSchema,
                eventRouter);
    }
}
