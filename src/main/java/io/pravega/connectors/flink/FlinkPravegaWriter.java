/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TxnFailedException;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Flink sink implementation for writing into pravega storage.
 *
 * @param <T> The type of the event to be written.
 */
@Slf4j
public class FlinkPravegaWriter<T>
        extends TwoPhaseCommitSinkFunction<T, FlinkPravegaWriter.PravegaTransactionState, Void> {

    private static final long serialVersionUID = 1L;

    private static final String PRAVEGA_WRITER_METRICS_GROUP = "PravegaWriter";

    private static final String SCOPED_STREAM_METRICS_GAUGE = "stream";

    // ----------- Runtime fields ----------------

    // Error which will be detected asynchronously and reported to Flink
    @VisibleForTesting
    volatile AtomicReference<Throwable> writeError = new AtomicReference<>(null);

    // Used to track confirmation from all writes to ensure guaranteed writes.
    @VisibleForTesting
    AtomicLong pendingWritesCount = new AtomicLong();

    private ExecutorService executorService;

    private long currentWatermark = Long.MIN_VALUE;

    // ----------- configuration fields -----------

    // flag to enable/disable metrics
    private final boolean enableMetrics;

    // The Pravega client config.
    private final ClientConfig clientConfig;

    // The supplied event serializer.
    private final SerializationSchema<T> serializationSchema;

    // The router used to partition events within a stream.
    private final PravegaEventRouter<T> eventRouter;

    // The destination stream.
    @SuppressFBWarnings("SE_BAD_FIELD")
    private final Stream stream;

    // Various timeouts
    private final long txnLeaseRenewalPeriod;

    // The sink's mode of operation. This is used to provide different guarantees for the written events.
    private PravegaWriterMode writerMode;

    // flag to enable/disable watermark
    private boolean enableWatermark;

    // Client factory for PravegaWriter instances
    private transient EventStreamClientFactory clientFactory = null;

    // Pravega writer instance
    private transient EventStreamWriter<T> writer = null;

    // Transactional Pravega writer instance
    private transient TransactionalEventStreamWriter<T> transactionalWriter = null;

    // Pravega Writer prefix that will be used by all Pravega Writers in this Sink
    private String writerIdPrefix;

    /**
     * The flink pravega writer instance which can be added as a sink to a Flink job.
     *
     * @param clientConfig          The Pravega client configuration.
     * @param stream                The destination stream.
     * @param serializationSchema   The implementation for serializing every event into pravega's storage format.
     * @param eventRouter           The implementation to extract the partition key from the event.
     * @param writerMode            The Pravega writer mode.
     * @param txnLeaseRenewalPeriod Transaction lease renewal period in milliseconds.
     * @param enableWatermark       Flag to indicate whether Pravega watermark needs to be enabled or not.
     * @param enableMetrics         Flag to indicate whether metrics needs to be enabled or not.
     */
    protected FlinkPravegaWriter(
            final ClientConfig clientConfig,
            final Stream stream,
            final SerializationSchema<T> serializationSchema,
            final PravegaEventRouter<T> eventRouter,
            final PravegaWriterMode writerMode,
            final long txnLeaseRenewalPeriod,
            final boolean enableWatermark,
            final boolean enableMetrics) {

        super(new TransactionStateSerializer(), VoidSerializer.INSTANCE);
        this.clientConfig = Preconditions.checkNotNull(clientConfig, "clientConfig");
        this.stream = Preconditions.checkNotNull(stream, "stream");
        this.serializationSchema = Preconditions.checkNotNull(serializationSchema, "serializationSchema");
        this.eventRouter = Preconditions.checkNotNull(eventRouter, "eventRouter");
        this.writerMode = Preconditions.checkNotNull(writerMode, "writerMode");
        Preconditions.checkArgument(txnLeaseRenewalPeriod > 0, "txnLeaseRenewalPeriod must be > 0");
        this.txnLeaseRenewalPeriod = txnLeaseRenewalPeriod;
        this.enableWatermark = enableWatermark;
        this.enableMetrics = enableMetrics;
        this.writerIdPrefix = UUID.randomUUID().toString();
    }

    /**
     * Gets the associated event router.
     */
    public PravegaEventRouter<T> getEventRouter() {
        return this.eventRouter;
    }

    /**
     * Gets this writer's operating mode.
     */
    public PravegaWriterMode getPravegaWriterMode() {
        return this.writerMode;
    }

    /**
     * Gets this enable watermark flag.
     */
    public boolean getEnableWatermark() {
        return this.enableWatermark;
    }

    // ------------------------------------------------------------------------

    @Override
    public void open(Configuration configuration) throws Exception {
        initializeInternalWriter();
        log.info("Initialized Pravega writer {} for stream: {} with controller URI: {}", writerId(), stream, clientConfig.getControllerURI());
        if (enableMetrics) {
            registerMetrics();
        }
    }

    @Override
    protected void invoke(PravegaTransactionState transaction, T event, Context context) throws Exception {
        checkWriteError();

        switch (writerMode) {
            case EXACTLY_ONCE:
                transaction.getTransaction().writeEvent(eventRouter.getRoutingKey(event), event);
                if (enableWatermark) {
                    transaction.watermark = context.currentWatermark();
                }
                break;
            case ATLEAST_ONCE:
            case BEST_EFFORT:
                this.pendingWritesCount.incrementAndGet();
                final CompletableFuture<Void> future = writer.writeEvent(eventRouter.getRoutingKey(event), event);
                if (enableWatermark && shouldEmitWatermark(currentWatermark, context)) {
                    writer.noteTime(context.currentWatermark());
                    currentWatermark = context.currentWatermark();
                }
                future.whenCompleteAsync(
                        (result, e) -> {
                            if (e != null) {
                                log.warn("Detected a write failure: {}", e);

                                // We will record only the first error detected, since this will mostly likely help with
                                // finding the root cause. Storing all errors will not be feasible.
                                writeError.compareAndSet(null, e);
                            }
                            synchronized (this) {
                                pendingWritesCount.decrementAndGet();
                                this.notify();
                            }
                        },
                        executorService
                );
                break;
            default:
                throw new UnsupportedOperationException("Not implemented writer mode");
        }
    }

    @Override
    protected PravegaTransactionState beginTransaction() throws Exception {
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

    @Override
    protected void preCommit(PravegaTransactionState transaction) throws Exception {
        switch (writerMode) {
            case EXACTLY_ONCE:
                transaction.getTransaction().flush();
                break;
            case ATLEAST_ONCE:
                flushAndVerify();
                break;
            case BEST_EFFORT:
                break;
            default:
                throw new UnsupportedOperationException("Not implemented writer mode");
        }
    }

    @Override
    protected void commit(PravegaTransactionState transaction) {
        switch (writerMode) {
            case EXACTLY_ONCE:
                final Transaction<T> txn = transaction.getTransaction() != null ? transaction.getTransaction() :
                        transactionalWriter.getTxn(UUID.fromString(transaction.transactionId));
                final Transaction.Status status = txn.checkStatus();
                try {
                    if (status == Transaction.Status.OPEN) {
                        if (enableWatermark && transaction.watermark != null) {
                            txn.commit(transaction.watermark);
                        } else {
                            txn.commit();
                        }
                    } else {
                        log.warn("{} - Transaction {} has unexpected transaction status {} while committing",
                                writerId(), txn.getTxnId(), status);
                    }
                } catch (TxnFailedException e) {
                    log.error("{} - Transaction {} commit failed.", writerId(), txn.getTxnId());
                }
                break;
            case ATLEAST_ONCE:
            case BEST_EFFORT:
                break;
            default:
                throw new UnsupportedOperationException("Not implemented writer mode");
        }
    }

    @Override
    protected void recoverAndCommit(PravegaTransactionState transaction) {
        initializeInternalWriter();
        commit(transaction);
    }

    @Override
    protected void abort(PravegaTransactionState transaction) {
        switch (writerMode) {
            case EXACTLY_ONCE:
                final Transaction<T> txn = transaction.getTransaction() != null ? transaction.getTransaction() :
                        transactionalWriter.getTxn(UUID.fromString(transaction.transactionId));
                txn.abort();
                break;
            case ATLEAST_ONCE:
            case BEST_EFFORT:
                break;
            default:
                throw new UnsupportedOperationException("Not implemented writer mode");
        }
    }

    @Override
    protected void recoverAndAbort(PravegaTransactionState transaction) {
        initializeInternalWriter();
        abort(transaction);
    }

    @Override
    public void close() throws Exception {
        Exception exception = null;

        try {
            // Current transaction will be aborted with this method
            super.close();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        if (writer != null) {
            try {
                flushAndVerify();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

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

        if (transactionalWriter != null) {
            try {
                transactionalWriter.close();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }
        }

        if (clientFactory != null) {
            try {
                clientFactory.close();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }
        }

        if (exception != null) {
            throw exception;
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

    /**
     * register metrics
     *
     */
    private void registerMetrics() {
        MetricGroup pravegaWriterMetricGroup = getRuntimeContext().getMetricGroup().addGroup(PRAVEGA_WRITER_METRICS_GROUP);
        pravegaWriterMetricGroup.gauge(SCOPED_STREAM_METRICS_GAUGE, new StreamNameGauge(stream.getScopedName()));
    }

    // ------------------------------------------------------------------------
    //  helper methods
    // ------------------------------------------------------------------------

    private void checkWriteError() throws Exception {
        Throwable error = this.writeError.getAndSet(null);
        if (error != null) {
            throw new IOException("Write failure", error);
        }
    }

    @VisibleForTesting
    void flushAndVerify() throws Exception {
        writer.flush();

        // Wait until all errors, if any, have been recorded.
        synchronized (this) {
            while (this.pendingWritesCount.get() > 0) {
                this.wait();
            }
        }

        checkWriteError();
    }

    @VisibleForTesting
    protected EventStreamClientFactory createClientFactory(String scopeName, ClientConfig clientConfig) {
        return EventStreamClientFactory.withScope(scopeName, clientConfig);
    }

    @VisibleForTesting
    protected void createInternalWriter() {
        Preconditions.checkState(this.clientFactory != null, "clientFactory not initialized");
        Serializer<T> eventSerializer = new FlinkSerializer<>(serializationSchema);
        EventWriterConfig writerConfig = EventWriterConfig.builder()
                .transactionTimeoutTime(txnLeaseRenewalPeriod)
                .build();
        if (this.writerMode == PravegaWriterMode.EXACTLY_ONCE) {
            transactionalWriter = clientFactory.createTransactionalEventWriter(writerId(), stream.getStreamName(), eventSerializer, writerConfig);
        } else {
            executorService = createExecutorService();
            writer = clientFactory.createEventWriter(writerId(), stream.getStreamName(), eventSerializer, writerConfig);
        }
    }

    boolean shouldEmitWatermark(long watermark, Context context) {
        return context.currentWatermark() > Long.MIN_VALUE && context.currentWatermark() < Long.MAX_VALUE &&
                watermark < context.currentWatermark() && context.timestamp() >= context.currentWatermark();
    }

    @VisibleForTesting
    protected ExecutorService createExecutorService() {
        return Executors.newSingleThreadExecutor();
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

        this.clientFactory = createClientFactory(stream.getScope(), clientConfig);
        createInternalWriter();
    }

    private boolean isCheckpointEnabled() {
        return ((StreamingRuntimeContext) getRuntimeContext()).isCheckpointingEnabled();
    }

    protected String writerId() {
        return writerIdPrefix + "-" + getRuntimeContext().getIndexOfThisSubtask();
    }

    public static <T> FlinkPravegaWriter.Builder<T> builder() {
        return new Builder<>();
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

    // ------------------------------------------------------------------------
    //  State and context classes and serializers
    // ------------------------------------------------------------------------

    /*
     * Pending transaction state snapshot representing combinations of transaction id
     */
    static class PravegaTransactionState {
        private transient Transaction transaction;
        private String transactionId;
        private Long watermark;

        PravegaTransactionState() {
            this(null);
        }

        PravegaTransactionState(Transaction transaction) {
            this(transaction, null);
        }

        PravegaTransactionState(Transaction transaction, Long watermark) {
            this.transaction = transaction;
            if (transaction != null) {
                this.transactionId = transaction.getTxnId().toString();
            }
            this.watermark = watermark;
        }

        PravegaTransactionState(String transactionId, Long watermark) {
            this.transactionId = transactionId;
            this.watermark = watermark;
        }

        Transaction getTransaction() {
            return transaction;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PravegaTransactionState that = (PravegaTransactionState) o;
            return Objects.equals(transactionId, that.transactionId) &&
                    Objects.equals(watermark, that.watermark);
        }

        @Override
        public int hashCode() {
            return Objects.hash(transactionId, watermark);
        }
    }

    /**
     * {@link org.apache.flink.api.common.typeutils.TypeSerializer} for
     * {@link FlinkPravegaWriter.PravegaTransactionState}.
     */
    @VisibleForTesting
    @Internal
    public static class TransactionStateSerializer extends TypeSerializerSingleton<FlinkPravegaWriter.PravegaTransactionState> {

        private static final long serialVersionUID = 1L;

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public FlinkPravegaWriter.PravegaTransactionState createInstance() {
            return null;
        }

        @Override
        public FlinkPravegaWriter.PravegaTransactionState copy(FlinkPravegaWriter.PravegaTransactionState from) {
            return from;
        }

        @Override
        public FlinkPravegaWriter.PravegaTransactionState copy(FlinkPravegaWriter.PravegaTransactionState from,
                                                               FlinkPravegaWriter.PravegaTransactionState reuse) {
            return from;
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            boolean hasTransactionId = source.readBoolean();
            target.writeBoolean(hasTransactionId);
            if (hasTransactionId) {
                target.writeUTF(source.readUTF());
            }
            boolean hasWatermark = source.readBoolean();
            target.writeBoolean(hasWatermark);
            if (hasWatermark) {
                target.writeLong(source.readLong());
            }
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(FlinkPravegaWriter.PravegaTransactionState record,
                              DataOutputView target) throws IOException {
            if (record.transactionId == null) {
                target.writeBoolean(false);
            } else {
                target.writeBoolean(true);
                target.writeUTF(record.transactionId);
            }
            if (record.watermark == null) {
                target.writeBoolean(false);
            } else {
                target.writeBoolean(true);
                target.writeLong(record.watermark);
            }
        }

        @Override
        public FlinkPravegaWriter.PravegaTransactionState deserialize(DataInputView source) throws IOException {
            String transactionalId = null;
            if (source.readBoolean()) {
                transactionalId = source.readUTF();
            }
            Long watermark = null;
            if (source.readBoolean()) {
                watermark = source.readLong();
            }
            return new FlinkPravegaWriter.PravegaTransactionState(transactionalId, watermark);
        }

        @Override
        public FlinkPravegaWriter.PravegaTransactionState deserialize(
                FlinkPravegaWriter.PravegaTransactionState reuse,
                DataInputView source) throws IOException {
            return deserialize(source);
        }

        // -----------------------------------------------------------------------------------

        @Override
        public TypeSerializerSnapshot<FlinkPravegaWriter.PravegaTransactionState> snapshotConfiguration() {
            return new TransactionStateSerializerSnapshot();
        }

        /**
         * Serializer configuration snapshot for compatibility and format evolution.
         */
        @SuppressWarnings("WeakerAccess")
        public static final class TransactionStateSerializerSnapshot extends
                SimpleTypeSerializerSnapshot<FlinkPravegaWriter.PravegaTransactionState> {

            public TransactionStateSerializerSnapshot() {
                super(TransactionStateSerializer::new);
            }
        }
    }


    // ------------------------------------------------------------------------
    //  builder
    // ------------------------------------------------------------------------

    /**
     * A builder for {@link FlinkPravegaWriter}.
     *
     * @param <T> the element type.
     */
    public static class Builder<T> extends AbstractStreamingWriterBuilder<T, Builder<T>> {

        private SerializationSchema<T> serializationSchema;

        private PravegaEventRouter<T> eventRouter;

        protected Builder<T> builder() {
            return this;
        }

        /**
         * Sets the serialization schema.
         *
         * @param serializationSchema The serialization schema
         */
        public Builder<T> withSerializationSchema(SerializationSchema<T> serializationSchema) {
            this.serializationSchema = serializationSchema;
            return builder();
        }

        /**
         * Sets the event router.
         *
         * @param eventRouter the event router which produces a key per event.
         */
        public Builder<T> withEventRouter(PravegaEventRouter<T> eventRouter) {
            this.eventRouter = eventRouter;
            return builder();
        }

        /**
         * Builds the {@link FlinkPravegaWriter}.
         */
        public FlinkPravegaWriter<T> build() {
            Preconditions.checkState(eventRouter != null, "Event router must be supplied.");
            Preconditions.checkState(serializationSchema != null, "Serialization schema must be supplied.");
            return createSinkFunction(serializationSchema, eventRouter);
        }
    }
}
