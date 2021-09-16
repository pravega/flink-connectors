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

@Slf4j
public class PravegaWriter<T> implements SinkWriter<T, PravegaTransactionState<T>, Void> {

    private static final long serialVersionUID = 1L;

    private static final String PRAVEGA_WRITER_METRICS_GROUP = "PravegaWriter";

    private static final String SCOPED_STREAM_METRICS_GAUGE = "stream";

    private final Sink.InitContext sinkInitContext;

    // ----------- Runtime fields ----------------

    // Error which will be detected asynchronously and reported to Flink
    @VisibleForTesting
    volatile AtomicReference<Throwable> writeError = new AtomicReference<>(null);

    // Used to track confirmation from all writes to ensure guaranteed writes.
    @VisibleForTesting
    AtomicLong pendingWritesCount = new AtomicLong();

    // ----------- configuration fields -----------

    // The Pravega client config.
    private final ClientConfig clientConfig;

    // The destination stream.
    @SuppressFBWarnings("SE_BAD_FIELD")
    private final Stream stream;

    // Various timeouts
    private final long txnLeaseRenewalPeriod;

    // The sink's mode of operation. This is used to provide different guarantees for the written events.
    private final PravegaWriterMode writerMode;

    // flag to enable/disable watermark
    private final boolean enableWatermark;

    // Pravega Writer Id
    private final String writerId;

    private final SerializationSchema<T> serializationSchema;

    private final List<FlinkPravegaInternalWriter<T>> writers = new ArrayList<>();

    private final transient FlinkPravegaInternalWriter<T> currentWriter;

    public PravegaWriter(Sink.InitContext sinkInitContext,
                         boolean enableMetrics, ClientConfig clientConfig,
                         Stream stream,
                         long txnLeaseRenewalPeriod,
                         final PravegaWriterMode writerMode,
                         boolean enableWatermark,
                         SerializationSchema<T> serializationSchema,
                         PravegaEventRouter<T> eventRouter) {
        this.sinkInitContext = sinkInitContext;
        this.clientConfig = clientConfig;
        this.stream = stream;
        this.txnLeaseRenewalPeriod = txnLeaseRenewalPeriod;
        this.writerMode = writerMode;
        this.enableWatermark = enableWatermark;
        this.serializationSchema = serializationSchema;
        this.writerId = UUID.randomUUID() + "-" + sinkInitContext.getSubtaskId();

        // the (transactional) pravega writer is initialized
        // in FlinkPravegaInternalWriter#createInternalWriter
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
    public List<PravegaTransactionState<T>> prepareCommit(boolean flush) throws IOException {
        if (!flush) {
        }

        final List<PravegaTransactionState<T>> committables;
        try {
            switch (writerMode) {
                case EXACTLY_ONCE:
                    PravegaTransactionState transaction = PravegaTransactionState.of(currentWriter);
                    committables = Collections.singletonList(transaction);
                    transaction.getTransaction().flush();
                    writers.add(currentWriter);
                    break;
                case ATLEAST_ONCE:
                    currentWriter.flushAndVerify();
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
