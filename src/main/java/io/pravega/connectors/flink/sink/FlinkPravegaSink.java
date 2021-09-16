package io.pravega.connectors.flink.sink;

import io.pravega.client.ClientConfig;
import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.connectors.flink.PravegaWriterMode;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class FlinkPravegaSink<T> implements Sink<T, PravegaTransactionState<T>, Void, Void> {

    // flag to enable/disable metrics
    private final boolean enableMetrics;

    // The Pravega client config.
    private final ClientConfig clientConfig;

    private final Stream stream;

    // Various timeouts
    private final long txnLeaseRenewalPeriod;

    // The sink's mode of operation. This is used to provide different guarantees for the written events.
    private final PravegaWriterMode writerMode;

    // flag to enable/disable watermark
    private final boolean enableWatermark;

    private final SerializationSchema<T> serializationSchema;

    // The router used to partition events within a stream, can be null for random routing
    private final PravegaEventRouter<T> eventRouter;

    public FlinkPravegaSink(boolean enableMetrics, ClientConfig clientConfig, Stream stream, long txnLeaseRenewalPeriod, PravegaWriterMode writerMode, boolean enableWatermark, SerializationSchema<T> serializationSchema, PravegaEventRouter<T> eventRouter) {
        this.enableMetrics = enableMetrics;
        this.clientConfig = clientConfig;
        this.stream = stream;
        this.txnLeaseRenewalPeriod = txnLeaseRenewalPeriod;
        this.writerMode = writerMode;
        this.enableWatermark = enableWatermark;
        this.serializationSchema = serializationSchema;
        this.eventRouter = eventRouter;
    }

    @Override
    public SinkWriter<T, PravegaTransactionState<T>, Void> createWriter(
            InitContext context, List<Void> states) throws IOException {
        return new PravegaWriter<>(
                context,
                enableMetrics,
                clientConfig,
                stream,
                txnLeaseRenewalPeriod,
                writerMode,
                enableWatermark,
                serializationSchema,
                eventRouter);
    }

    @Override
    public Optional<Committer<PravegaTransactionState<T>>> createCommitter() throws IOException {
        return Optional.of(new PravegaCommitter<>(clientConfig,
                txnLeaseRenewalPeriod, stream, writerMode, enableWatermark, serializationSchema, eventRouter));
    }

    @Override
    public Optional<GlobalCommitter<PravegaTransactionState<T>, Void>> createGlobalCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<PravegaTransactionState<T>>> getCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getWriterStateSerializer() {
        return Optional.empty();
    }
}
