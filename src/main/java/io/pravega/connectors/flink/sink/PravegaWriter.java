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

import edu.umd.cs.findbugs.annotations.Nullable;
import io.pravega.client.ClientConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.connectors.flink.PravegaWriterMode;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class PravegaWriter<T> implements SinkWriter<T, PravegaTransactionState, Void> {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkPravegaWriter.class);

    private static final long serialVersionUID = 1L;

    private static final String PRAVEGA_WRITER_METRICS_GROUP = "PravegaWriter";

    private static final String SCOPED_STREAM_METRICS_GAUGE = "stream";

    // The sink's mode of operation. This is used to provide different guarantees for the written events.
    private final PravegaWriterMode writerMode;

    // The writer we use for create and begin a transaction.
    // However, the transaction is committed in PravegaCommitter via a re-created FlinkPravegaInternalWriter.
    private FlinkPravegaInternalWriter<T> currentWriter;

    // Place where we hold writers for different checkpoints.
    private final List<FlinkPravegaInternalWriter<T>> writers = new ArrayList<>();

    // --------- configuration for creating a FlinkPravegaInternalWriter ---------

    private final ClientConfig clientConfig;
    private final Stream stream;
    private final long txnLeaseRenewalPeriod;
    private final boolean enableWatermark;
    private final SerializationSchema<T> serializationSchema;
    // The router used to partition events within a stream, can be null for random routing
    @Nullable
    private final PravegaEventRouter<T> eventRouter;

    public PravegaWriter(Sink.InitContext context,
                         boolean enableMetrics,
                         ClientConfig clientConfig,
                         Stream stream,
                         long txnLeaseRenewalPeriod,
                         final PravegaWriterMode writerMode,
                         boolean enableWatermark,
                         SerializationSchema<T> serializationSchema,
                         @Nullable PravegaEventRouter<T> eventRouter) {
        this.writerMode = writerMode;

        this.clientConfig = clientConfig;
        this.stream = stream;
        this.txnLeaseRenewalPeriod = txnLeaseRenewalPeriod;
        this.enableWatermark = enableWatermark;
        this.serializationSchema = serializationSchema;
        this.eventRouter = eventRouter;

        // the (transactional) pravega writer is initialized
        // in FlinkPravegaInternalWriter#createInternalWriter
        this.currentWriter = new FlinkPravegaInternalWriter<>(clientConfig, stream,
                txnLeaseRenewalPeriod, writerMode, enableWatermark, serializationSchema, eventRouter);

        if (this.writerMode == PravegaWriterMode.EXACTLY_ONCE) {
            this.currentWriter.beginTransaction();

            writers.add(currentWriter);
        }

        if (enableMetrics) {
            MetricGroup pravegaWriterMetricGroup = context.metricGroup().addGroup(PRAVEGA_WRITER_METRICS_GROUP);
            pravegaWriterMetricGroup.gauge(SCOPED_STREAM_METRICS_GAUGE, new StreamNameGauge(stream.getScopedName()));
        }
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
        final List<PravegaTransactionState> transactionStates;
        try {
            if (flush) {
                currentWriter.flushAndVerify();
            }

            switch (writerMode) {
                case EXACTLY_ONCE:
                    currentWriter = new FlinkPravegaInternalWriter<>(clientConfig, stream,
                            txnLeaseRenewalPeriod, writerMode, enableWatermark,
                            serializationSchema, eventRouter);
                    currentWriter.beginTransaction();

                    transactionStates = writers.stream().map(PravegaTransactionState::of).collect(Collectors.toList());
                    writers.clear();
                    break;
                case ATLEAST_ONCE:
                case BEST_EFFORT:
                    transactionStates = new ArrayList<>();
                    break;
                default:
                    throw new UnsupportedOperationException("Not implemented writer mode");
            }
        } catch (InterruptedException | TxnFailedException e) {
            throw new IOException("", e);
        }
        LOG.info("Committing {} committables, final commit={}.", transactionStates, flush);
        return transactionStates;
    }

    @Override
    public List<Void> snapshotState(long checkpointId) throws IOException {
        try {
            currentWriter.flushAndVerify();
        } catch (InterruptedException | TxnFailedException e) {
            throw new IOException(e);
        }

        switch (writerMode) {
            case EXACTLY_ONCE:
                if (currentWriter.isInTransaction()) {
                    writers.add(currentWriter);
                }
                currentWriter = new FlinkPravegaInternalWriter<>(clientConfig, stream,
                        txnLeaseRenewalPeriod, writerMode, enableWatermark, serializationSchema, eventRouter);
                currentWriter.beginTransaction();
                break;
            case ATLEAST_ONCE:
            case BEST_EFFORT:
                break;
            default:
                throw new UnsupportedOperationException("Not implemented writer mode");
        }

        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        LOG.info("Calling close from PravegaWriter");
        currentWriter.close();
    }

    /**
     * Wrap the SerializationSchema to a Pravega compatible Serializer.
     * @param <T> The type of the event.
     */
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
