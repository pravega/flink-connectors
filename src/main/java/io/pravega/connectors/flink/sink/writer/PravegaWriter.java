package io.pravega.connectors.flink.sink.writer;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.*;
import io.pravega.connectors.flink.PravegaWriterMode;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class PravegaWriter<T> implements SinkWriter<T, Transaction<T>, Void> {

    // ----------- Runtime fields ----------------

    private transient ExecutorService executorService;

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

    private final SerializationSchema<T> serializationSchema;

    private final List<PravegaTransactionState> producers = new ArrayList<>();

    private transient PravegaTransactionState currentProducer;

    // Pravega writer instance
    private transient EventStreamWriter<T> writer = null;

    // Transactional Pravega writer instance
    private transient TransactionalEventStreamWriter<T> transactionalWriter = null;

    public PravegaWriter(ClientConfig clientConfig, Stream stream, long txnLeaseRenewalPeriod, final PravegaWriterMode writerMode, SerializationSchema<T> serializationSchema) {
        this.clientConfig = clientConfig;
        this.stream = stream;
        this.txnLeaseRenewalPeriod = txnLeaseRenewalPeriod;
        this.writerMode = writerMode;
        this.serializationSchema = serializationSchema;
        this.currentProducer = beginTransaction();
        producers.add(currentProducer);
    }

    @Override
    public void write(T element, Context context) throws IOException {

    }

    @Override
    public List<Transaction<T>> prepareCommit(boolean flush) throws IOException {
        if (!flush) {
            currentProducer = beginTransaction();
        }

        final List<Transaction<T>> committables;
        try{
            switch (writerMode) {
                case EXACTLY_ONCE:
                    Transaction<T> transaction = currentProducer.getTransaction();
                    committables = producers.stream().map()
                    transaction.flush();
                    producers.add(currentProducer);
                    break;
                case ATLEAST_ONCE:
                    flushAndVerify();
                case BEST_EFFORT:
                    committables = new ArrayList<>();
                    break;
                default:
                    throw new UnsupportedOperationException("Not implemented writer mode");
            }
        } catch (Exception e){
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
        Exception exception = null;

        if (writer != null){
            try {
                writer.close();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            try {
                executorService.shutdown();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    private PravegaTransactionState beginTransaction() {
        initializeInternalWriter();
        switch (writerMode) {
            case EXACTLY_ONCE:
                Transaction<T> txn = transactionalWriter.beginTxn();
                return new PravegaTransactionState(txn);
            case ATLEAST_ONCE:
            case BEST_EFFORT:
                return new PravegaTransactionState();
            default:
                throw new UnsupportedOperationException("Not implemented writer mode");
        }
    }

    private void initializeInternalWriter() {
        if (this.writerMode == PravegaWriterMode.EXACTLY_ONCE) {
            if (this.transactionalWriter != null) {
                return;
            }
        } else {
            if (this.writer != null) {
                return;
            }
        }

        if (this.writerMode == PravegaWriterMode.EXACTLY_ONCE && !isCheckpointEnabled()) {
            // Pravega transaction writer (exactly-once) implementation can be used only when checkpoint is enabled
            throw new UnsupportedOperationException("Enable checkpointing to use the exactly-once writer mode.");
        }

        createInternalWriter(createClientFactory(stream.getScope(), clientConfig));
    }

    protected void createInternalWriter(EventStreamClientFactory clientFactory) {
        Serializer<T> eventSerializer = new FlinkSerializer<>(serializationSchema);
        EventWriterConfig writerConfig = EventWriterConfig.builder()
                .transactionTimeoutTime(txnLeaseRenewalPeriod)
                .build();
        if (this.writerMode == PravegaWriterMode.EXACTLY_ONCE) {
            transactionalWriter = clientFactory.createTransactionalEventWriter(writerId(), stream.getStreamName(), eventSerializer, writerConfig);
        } else {
            executorService = Executors.newSingleThreadExecutor();
            writer = clientFactory.createEventWriter(writerId(), stream.getStreamName(), eventSerializer, writerConfig);
        }
    }

    @VisibleForTesting
    void flushAndVerify() throws InterruptedException {
        writer.flush();

        // Wait until all errors, if any, have been recorded.
        synchronized (this) {
            while (this.pendingWritesCount.get() > 0) {
                this.wait();
            }
        }

        // TODO: check internal error on future.whenCompleteAsync
        // checkWriteError();
    }

    protected EventStreamClientFactory createClientFactory(String scopeName, ClientConfig clientConfig) {
        return EventStreamClientFactory.withScope(scopeName, clientConfig);
    }

    private boolean isCheckpointEnabled() {
        return ((StreamingRuntimeContext) getRuntimeContext()).isCheckpointingEnabled();
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
}
