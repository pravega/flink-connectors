package io.pravega.connectors.flink.sink;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.pravega.client.ClientConfig;
import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.connectors.flink.PravegaWriterMode;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.Committer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

@Slf4j
public class PravegaCommitter<T> implements Committer<PravegaTransactionState<T>> {
    // The Pravega client config.
    private final ClientConfig clientConfig;

    // Various timeouts
    private final long txnLeaseRenewalPeriod;

    // The destination stream.
    @SuppressFBWarnings("SE_BAD_FIELD")
    private final Stream stream;

    // The sink's mode of operation. This is used to provide different guarantees for the written events.
    private final PravegaWriterMode writerMode;

    // flag to enable/disable watermark
    private final boolean enableWatermark;

    private final SerializationSchema<T> serializationSchema;

    // The router used to partition events within a stream, can be null for random routing
    private final PravegaEventRouter<T> eventRouter;

    public PravegaCommitter(ClientConfig clientConfig,
                            long txnLeaseRenewalPeriod,
                            Stream stream,
                            PravegaWriterMode writerMode,
                            boolean enableWatermark,
                            SerializationSchema<T> serializationSchema, PravegaEventRouter<T> eventRouter) {
        this.clientConfig = clientConfig;
        this.txnLeaseRenewalPeriod = txnLeaseRenewalPeriod;
        this.stream = stream;
        this.writerMode = writerMode;
        this.enableWatermark = enableWatermark;
        this.serializationSchema = serializationSchema;
        this.eventRouter = eventRouter;
    }

    @Override
    public List<PravegaTransactionState<T>> commit(List<PravegaTransactionState<T>> committables) throws IOException {
        committables.forEach(transaction -> {
            FlinkPravegaInternalWriter<T> writer = new FlinkPravegaInternalWriter<>(
                    clientConfig, txnLeaseRenewalPeriod, stream, writerMode, enableWatermark,
                    serializationSchema, eventRouter, transaction.getWriterId());
            writer.commitTransaction();
        });
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        // Do nothing.
    }
}
