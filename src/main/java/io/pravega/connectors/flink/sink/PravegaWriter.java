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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.pravega.client.ClientConfig;
import io.pravega.client.stream.*;
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.connectors.flink.PravegaWriterMode;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
public class PravegaWriter<T> implements SinkWriter<T, PravegaTransactionState, Void> {

    private static final long serialVersionUID = 1L;

    private static final String PRAVEGA_WRITER_METRICS_GROUP = "PravegaWriter";

    private static final String SCOPED_STREAM_METRICS_GAUGE = "stream";

    private final Sink.InitContext sinkInitContext;

    // The sink's mode of operation. This is used to provide different guarantees for the written events.
    private final PravegaWriterMode writerMode;

    private final List<FlinkPravegaInternalWriter<T>> writers = new ArrayList<>();

    private final FlinkPravegaInternalWriter<T> currentWriter;

    public PravegaWriter(Sink.InitContext sinkInitContext,
                         boolean enableMetrics,
                         ClientConfig clientConfig,
                         Stream stream,
                         long txnLeaseRenewalPeriod,
                         final PravegaWriterMode writerMode,
                         boolean enableWatermark,
                         SerializationSchema<T> serializationSchema,
                         PravegaEventRouter<T> eventRouter) {
        this.sinkInitContext = sinkInitContext;
        this.writerMode = writerMode;

        // the (transactional) pravega writer is initialized
        // in FlinkPravegaInternalWriter#createInternalWriter
        String writerId = UUID.randomUUID() + "-" + sinkInitContext.getSubtaskId();
        this.currentWriter = new FlinkPravegaInternalWriter<>(clientConfig,
                txnLeaseRenewalPeriod, stream, writerMode, enableWatermark, serializationSchema, eventRouter, writerId);
        if (this.writerMode == PravegaWriterMode.EXACTLY_ONCE) {
            this.currentWriter.beginTransaction();
        }

        if (enableMetrics) {
            MetricGroup pravegaWriterMetricGroup = this.sinkInitContext.metricGroup().addGroup(PRAVEGA_WRITER_METRICS_GROUP);
            pravegaWriterMetricGroup.gauge(SCOPED_STREAM_METRICS_GAUGE, new StreamNameGauge(stream.getScopedName()));
        }

        writers.add(currentWriter);
    }

    @Override
    public void write(T element, Context context) throws IOException {
        try {
            currentWriter.write(element, context);
        } catch (TxnFailedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public List<PravegaTransactionState> prepareCommit(boolean flush) throws IOException {
        final List<PravegaTransactionState> committables;
        try {
            currentWriter.flushAndVerify();

            if (!flush) {
                currentWriter.beginTransaction();
            }

            switch (writerMode) {
                case EXACTLY_ONCE:
                    committables = writers.stream().map(PravegaTransactionState::of).collect(Collectors.toList());
                    writers.clear();
                    break;
                case ATLEAST_ONCE:
                case BEST_EFFORT:
                    committables = new ArrayList<>();
                    break;
                default:
                    throw new UnsupportedOperationException("Not implemented writer mode");
            }
        } catch (InterruptedException | TxnFailedException e) {
            throw new IOException("", e);
        }
        log.info("Committing {} committables.", committables);
        return committables;
    }

    @Override
    public List<Void> snapshotState() throws IOException {
        return null;
    }

    @Override
    public void close() throws Exception {
        currentWriter.abort();
    }

    // ------------------------------------------------------------------------
    //  serializer
    // ------------------------------------------------------------------------

    @VisibleForTesting
    static final class FlinkSerializer<T> implements Serializer<T> {

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

    /**
     * Gauge for getting the fully qualified stream name information.
     */
    private static class StreamNameGauge implements Gauge<String> {

        final String stream;

        public StreamNameGauge(String stream) {
            this.stream = stream;
        }

        @Override
        public String getValue() {
            return stream;
        }
    }
}
