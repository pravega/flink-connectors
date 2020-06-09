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
import io.pravega.common.Exceptions;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

/**
 * Flink sink implementation for writing into pravega storage.
 *
 * @param <T> The type of the event to be written.
 */
@Slf4j
public class FlinkPravegaWriter<T>
        extends RichSinkFunction<T>
        implements ListCheckpointed<FlinkPravegaWriter.PendingTransaction>, CheckpointListener {

    private static final long serialVersionUID = 1L;

    // ----- metrics field constants -----

    private static final String PRAVEGA_WRITER_METRICS_GROUP = "PravegaWriter";

    private static final String SCOPED_STREAM_METRICS_GAUGE = "stream";

    // flag to enable/disable metrics
    final boolean enableMetrics;

    // Writer interface to assist exactly-once and at-least-once functionality
    @VisibleForTesting
    transient AbstractInternalWriter writer = null;

    // ----------- configuration fields -----------

    // The Pravega client config.
    final ClientConfig clientConfig;

    // The supplied event serializer.
    final SerializationSchema<T> serializationSchema;

    // The router used to partition events within a stream.
    final PravegaEventRouter<T> eventRouter;

    // The destination stream.
    @SuppressFBWarnings("SE_BAD_FIELD")
    final Stream stream;

    // Various timeouts
    private final long txnLeaseRenewalPeriod;

    // The sink's mode of operation. This is used to provide different guarantees for the written events.
    private PravegaWriterMode writerMode;

    // flag to enable/disable watermark
    private boolean enableWatermark;

    // Client factory for PravegaWriter instances
    private transient EventStreamClientFactory clientFactory = null;

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
    public void open(Configuration parameters) throws Exception {
        initializeInternalWriter();
        writer.open();
        log.info("Initialized Pravega writer {} for stream: {} with controller URI: {}", writerId(), stream, clientConfig.getControllerURI());
        if (enableMetrics) {
            registerMetrics();
        }
    }

    @Override
    public void invoke(T event, Context context) throws Exception {
        writer.write(event, context, enableWatermark);
    }

    @Override
    public void close() throws Exception {
        Exception exception = null;

        if (writer != null) {
            try {
                writer.close();
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

    @Override
    public List<PendingTransaction> snapshotState(long checkpointId, long checkpointTime) throws Exception {
        return writer.snapshotState(checkpointId, checkpointTime, enableWatermark);
    }

    /**
     * Restores the state, which here means the IDs of transaction for which we have
     * to ensure that they are really committed.
     *
     * Note: {@code restoreState} is called before {@code open}.
     */
    @Override
    public void restoreState(List<PendingTransaction> pendingTransactionList) throws Exception {
        initializeInternalWriter();
        writer.restoreState(pendingTransactionList);
    }

    /**
     * Notifies the writer that a checkpoint is complete.
     *
     * <p>This call happens when the checkpoint has been fully committed
     * (= second part of a two phase commit).
     *
     * <p>This method is called under a mutually exclusive lock from
     * the invoke() and trigger/restore methods, so there is no
     * need for additional synchronization.
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        writer.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {

    }

    // ------------------------------------------------------------------------
    //  helper methods
    // ------------------------------------------------------------------------

    @VisibleForTesting
    protected EventStreamClientFactory createClientFactory(String scopeName, ClientConfig clientConfig) {
        return EventStreamClientFactory.withScope(scopeName, clientConfig);
    }

    @VisibleForTesting
    protected AbstractInternalWriter createInternalWriter() {
        Preconditions.checkState(this.clientFactory != null, "clientFactory not initialized");
        if (this.writerMode == PravegaWriterMode.EXACTLY_ONCE) {
            return new TransactionalWriter(this.clientFactory);
        } else {
            ExecutorService executorService = createExecutorService();
            return new NonTransactionalWriter(this.clientFactory, executorService);
        }
    }

    @VisibleForTesting
    protected ExecutorService createExecutorService() {
        return Executors.newSingleThreadExecutor();
    }

    private void initializeInternalWriter() {
        if (this.writer != null) {
            return;
        }

        if (this.writerMode == PravegaWriterMode.EXACTLY_ONCE && !isCheckpointEnabled()) {
            // Pravega transaction writer (exactly-once) implementation can be used only when checkpoint is enabled
            throw new UnsupportedOperationException("Enable checkpointing to use the exactly-once writer mode.");
        }

        this.clientFactory = createClientFactory(stream.getScope(), clientConfig);
        this.writer = createInternalWriter();
    }

    private boolean isCheckpointEnabled() {
        return ((StreamingRuntimeContext) getRuntimeContext()).isCheckpointingEnabled();
    }

    protected String writerId() {
        return writerIdPrefix +"-"+getRuntimeContext().getIndexOfThisSubtask();
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
    //  utilities
    // ------------------------------------------------------------------------

    @VisibleForTesting
    static final class TransactionAndCheckpoint<T> {

        private final Transaction<T> transaction;
        private final long checkpointId;
        private final Long watermark;

        TransactionAndCheckpoint(Transaction<T> transaction, long checkpointId) {
            this.transaction = transaction;
            this.checkpointId = checkpointId;
            this.watermark = null;
        }

        TransactionAndCheckpoint(Transaction<T> transaction, long checkpointId, Long watermark) {
            this.transaction = transaction;
            this.checkpointId = checkpointId;
            this.watermark = watermark;
        }

        Transaction<T> transaction() {
            return transaction;
        }

        long checkpointId() {
            return checkpointId;
        }

        Long watermark() {
            return watermark;
        }

        @Override
        public String toString() {
            return "(checkpoint: " + checkpointId + ", transaction: " + transaction.getTxnId() + ", watermark: " + watermark() + ')';
        }
    }

    /*
     * An abstract internal writer.
     */
    @VisibleForTesting
    abstract class AbstractInternalWriter {

        @Getter
        private EventStreamWriter<T> pravegaWriter;

        @Getter
        private TransactionalEventStreamWriter<T> pravegaTxnWriter;

        @Getter
        @Setter
        private transient long watermark;

        AbstractInternalWriter(EventStreamClientFactory clientFactory, boolean txnWriter) {
            Serializer<T> eventSerializer = new FlinkSerializer<>(serializationSchema);
            EventWriterConfig writerConfig = EventWriterConfig.builder()
                    .transactionTimeoutTime(txnLeaseRenewalPeriod)
                    .build();
            watermark = Long.MIN_VALUE;
            if (txnWriter) {
                pravegaTxnWriter = clientFactory.createTransactionalEventWriter(writerId(), stream.getStreamName(), eventSerializer, writerConfig);
            } else {
                pravegaWriter = clientFactory.createEventWriter(writerId(), stream.getStreamName(), eventSerializer, writerConfig);
            }
        }

        boolean shouldEmitWatermark(Context context) {
            return context.currentWatermark() > Long.MIN_VALUE && context.currentWatermark() < Long.MAX_VALUE &&
                    watermark < context.currentWatermark() && context.timestamp() >= context.currentWatermark();
        }

        abstract void open() throws Exception;

        abstract void write(T event, Context context, boolean enableWatermark) throws Exception;

        void close() throws Exception {
            if (pravegaWriter != null) {
                pravegaWriter.close();
            }
            if (pravegaTxnWriter != null) {
                pravegaTxnWriter.close();
            }
        }

        abstract List<PendingTransaction> snapshotState(long checkpointId, long checkpointTime, boolean enableWatermark) throws Exception;

        abstract void restoreState(List<PendingTransaction> pendingTransactionList) throws Exception;

        abstract void notifyCheckpointComplete(long checkpointId) throws Exception;

    }

    /*
     * Implements an {@link AbstractInternalWriter} using transactional writes to Pravega.
     */
    @VisibleForTesting
    class TransactionalWriter extends AbstractInternalWriter {

        /**
         * The currently running transaction to which we write
         */
        @VisibleForTesting
        Transaction<T> currentTxn;

        /**
         * The transactions that are complete from Flink's view (their checkpoint was triggered),
         * but not fully committed, because their corresponding checkpoint is not yet confirmed
         */
        @VisibleForTesting
        final ArrayDeque<TransactionAndCheckpoint<T>> txnsPendingCommit;

        TransactionalWriter(EventStreamClientFactory clientFactory) {
            super(clientFactory, true);
            this.txnsPendingCommit = new ArrayDeque<>();
        }

        @Override
        public void open() throws Exception {
            // start the transaction that will hold the elements till the first checkpoint
            this.currentTxn = this.getPravegaTxnWriter().beginTxn();
            log.debug("{} - started first transaction '{}'", writerId(), this.currentTxn.getTxnId());
        }

        @Override
        public void write(T event, Context context, boolean enableWatermark) throws Exception {
            this.currentTxn.writeEvent(eventRouter.getRoutingKey(event), event);
            if (enableWatermark) {
                this.setWatermark(context.currentWatermark());
            }
        }

        @Override
        public void close() throws Exception {
            Exception exception = null;

            Transaction<?> txn = this.currentTxn;
            if (txn != null) {
                try {
                    Exceptions.handleInterrupted(txn::abort);
                } catch (Exception e) {
                    exception = e;
                }
            }

            try {
                super.close();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            if (exception != null) {
                throw exception;
            }
        }

        @Override
        public List<PendingTransaction> snapshotState(long checkpointId, long checkpointTime, boolean enableWatermark) throws Exception {
            // this is like the pre-commit of a 2-phase-commit transaction
            // we are ready to commit and remember the transaction

            final Transaction<T> txn = this.currentTxn;
            Preconditions.checkState(txn != null, "bug: no transaction object when performing state snapshot");

            log.debug("{} - checkpoint {} triggered, flushing transaction '{}'", writerId(), checkpointId, txn.getTxnId());

            // make sure all events go out
            txn.flush();

            // remember the transaction to be committed when the checkpoint is confirmed
            if (enableWatermark) {
                this.txnsPendingCommit.addLast(new TransactionAndCheckpoint<>(txn, checkpointId, this.getWatermark()));
            } else {
                this.txnsPendingCommit.addLast(new TransactionAndCheckpoint<>(txn, checkpointId));
            }

            // start the next transaction for what comes after this checkpoint
            this.currentTxn = this.getPravegaTxnWriter().beginTxn();

            log.debug("{} - started new transaction '{}'", writerId(), this.currentTxn.getTxnId());
            log.debug("{} - storing pending transactions {}", writerId(), txnsPendingCommit);

            // store all pending transactions in the checkpoint state
            return txnsPendingCommit.stream()
                    .map(v -> new PendingTransaction(v.transaction().getTxnId(), stream.getScope(), stream.getStreamName(), v.watermark()))
                    .collect(Collectors.toList());
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            // the following scenarios are possible here
            //
            //  (1) there is exactly one transaction from the latest checkpoint that
            //      was triggered and completed. That should be the common case.
            //      Simply commit that transaction in that case.
            //
            //  (2) there are multiple pending transactions because one previous
            //      checkpoint was skipped. That is a rare case, but can happen
            //      for example when:
            //
            //        - the master cannot persist the metadata of the last
            //          checkpoint (temporary outage in the storage system) but
            //          could persist a successive checkpoint (the one notified here)
            //
            //        - other (non Pravega sink) tasks could not persist their status during
            //          the previous checkpoint, but did not trigger a failure because they
            //          could hold onto their state and could successfully persist it in
            //          a successive checkpoint (the one notified here)
            //
            //      In both cases, the prior checkpoint never reach a committed state, but
            //      this checkpoint is always expected to subsume the prior one and cover all
            //      changes since the last successful one As a consequence, we need to commit
            //      all pending transactions.
            //
            //  (3) Multiple transactions are pending, but the checkpoint complete notification
            //      relates not to the latest. That is possible, because notification messages
            //      can be delayed (in an extreme case till arrive after a succeeding checkpoint
            //      was triggered) and because there can be concurrent overlapping checkpoints
            //      (a new one is started before the previous fully finished).
            //
            // ==> There should never be a case where we have no pending transaction here
            //

            Preconditions.checkState(!txnsPendingCommit.isEmpty(), "checkpoint completed, but no transaction pending");

            TransactionAndCheckpoint<T> txn;
            while ((txn = txnsPendingCommit.peekFirst()) != null && txn.checkpointId() <= checkpointId) {
                txnsPendingCommit.removeFirst();

                String watermarkMsg = txn.watermark == null ? "" : " at watermark "+txn.watermark;
                log.info("{} - checkpoint {} complete{}, committing completed checkpoint transaction {}",
                    writerId(), checkpointId, watermarkMsg, txn.transaction().getTxnId());

                // the big assumption is that this now actually works and that the transaction has not timed out, yet

                // TODO: currently, if this fails, there is actually data loss
                // see https://github.com/pravega/flink-connectors/issues/5
                if (txn.watermark() != null) {
                    txn.transaction().commit(txn.watermark());
                    log.debug("{} - committed checkpoint transaction {} at watermark {}", writerId(), txn.transaction().getTxnId(), txn.watermark());
                } else {
                    txn.transaction().commit();
                    log.debug("{} - committed checkpoint transaction {}", writerId(), txn.transaction().getTxnId());
                }
            }
        }

        @Override
        public void restoreState(List<PendingTransaction> pendingTransactionList) throws Exception {
            // when we are restored with a state (UUID,scope & stream), we don't really know whether the
            // transaction was already committed, or whether there was a failure between
            // completing the checkpoint on the master, and notifying the writer here.

            // (the common case is actually that is was already committed, the window
            // between the commit on the master and the notification here is very small)

            // it is possible to not have any transactions at all if there was a failure before
            // the first completed checkpoint, or in case of a scale-out event, where some of the
            // new task do not have and transactions assigned to check)

            // we can have more than one transaction to check in case of a scale-in event, or
            // for the reasons discussed in the 'notifyCheckpointComplete()' method.

            // we will create a writer per scope/stream from the pending transaction list and
            // commit the transaction if it is still open

            if (pendingTransactionList == null || pendingTransactionList.size() == 0) {
                return;
            }

            Exception exception = null;

            Map<Stream, List<PendingTransaction>> pendingTransactionsMap =
                    pendingTransactionList.stream().collect(groupingBy(s -> Stream.of(s.getScope(), s.getStream())));

            log.debug("pendingTransactionsMap:: " + pendingTransactionsMap);

            for (Map.Entry<Stream, List<PendingTransaction>> transactionsEntry: pendingTransactionsMap.entrySet()) {

                Stream streamId = transactionsEntry.getKey();
                String scope = streamId.getScope();
                String stream = streamId.getStreamName();

                Serializer<T> eventSerializer = new FlinkSerializer<>(serializationSchema);
                EventWriterConfig writerConfig = EventWriterConfig.builder()
                        .transactionTimeoutTime(txnLeaseRenewalPeriod)
                        .build();

                try (
                        EventStreamClientFactory restoreClientFactory = createClientFactory(scope, clientConfig);
                        TransactionalEventStreamWriter<T> restorePravegaWriter =
                                restoreClientFactory.createTransactionalEventWriter(writerId(),
                                        stream,
                                        eventSerializer,
                                        writerConfig);
                    ) {

                    log.info("restore state for the scope: {} and stream: {}", scope, stream);

                    List<PendingTransaction> pendingTransactions = transactionsEntry.getValue();

                    for (PendingTransaction pendingTransaction : pendingTransactions) {
                        UUID txnId = pendingTransaction.getUuid();
                        final Transaction<?> txn = restorePravegaWriter.getTxn(txnId);
                        final Transaction.Status status = txn.checkStatus();

                        if (status == Transaction.Status.OPEN) {
                            // that is the case when a crash happened between when the master committed
                            // the checkpoint, and the sink could be notified
                            log.debug("{} - committing completed checkpoint transaction {} at Watermark {} after task restore",
                                    writerId(), txnId, pendingTransaction.getWatermark());

                            if (pendingTransaction.getWatermark() != null) {
                                txn.commit(pendingTransaction.getWatermark());
                            } else {
                                txn.commit();
                            }

                            log.debug("{} - committed checkpoint transaction {}", writerId(), txnId);

                        } else if (status == Transaction.Status.COMMITTED || status == Transaction.Status.COMMITTING) {
                            // that the common case
                            log.debug("{} - at restore, transaction {} was already committed", writerId(), txnId);

                        } else {
                            log.warn("{} - found unexpected transaction status {} for transaction {} on task restore. " +
                                    "Transaction probably timed out between failure and restore. ", writerId(), status, txnId);
                        }
                    }
                } catch (Exception e) {
                    log.error("Exception occurred while restoring the state for scope: {} and stream: {}", scope, stream, e);
                }
            }

            if (exception != null) {
                throw exception;
            }

        }
    }

    /*
     * Implements an {@link AbstractInternalWriter} using non-transactional writes to Pravega.
     */
    @VisibleForTesting
    class NonTransactionalWriter extends AbstractInternalWriter {

        // Error which will be detected asynchronously and reported to Flink.
        @VisibleForTesting
        final AtomicReference<Throwable> writeError;

        // Used to track confirmation from all writes to ensure guaranteed writes.
        @VisibleForTesting
        final AtomicInteger pendingWritesCount;

        // Thread pool for handling callbacks from write events.
        private final ExecutorService executorService;

        NonTransactionalWriter(EventStreamClientFactory clientFactory, ExecutorService executorService) {
            super(clientFactory, false);
            this.writeError = new AtomicReference<>(null);
            this.pendingWritesCount = new AtomicInteger(0);
            this.executorService = executorService;
        }

        @Override
        public void open() throws Exception {
        }

        @Override
        public void write(T event, Context context, boolean enableWatermark) throws Exception {

            checkWriteError();

            this.pendingWritesCount.incrementAndGet();
            final CompletableFuture<Void> future = this.getPravegaWriter().writeEvent(eventRouter.getRoutingKey(event), event);
            if (enableWatermark && shouldEmitWatermark(context)) {
                this.getPravegaWriter().noteTime(context.currentWatermark());
                setWatermark(context.currentWatermark());
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
        }

        @Override
        public void close() throws Exception {
            Exception exception = null;

            try {
                flushAndVerify();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            try {
                super.close();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            try {
                this.executorService.shutdown();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            if (exception != null) {
                throw exception;
            }
        }

        @Override
        public List<PendingTransaction> snapshotState(long checkpointId, long checkpointTime, boolean enableWatermark) throws Exception {
            log.debug("Snapshot triggered, wait for all pending writes to complete");
            flushAndVerify();
            return new ArrayList<>();
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            //do nothing
        }

        @Override
        public void restoreState(List<PendingTransaction> pendingTransactionList) throws Exception {
            //do nothing
        }

        // Wait until all pending writes are completed and throw any errors detected.
        @VisibleForTesting
        void flushAndVerify() throws Exception {
            this.getPravegaWriter().flush();

            // Wait until all errors, if any, have been recorded.
            synchronized (this) {
                while (this.pendingWritesCount.get() > 0) {
                    this.wait();
                }
            }

            // Verify that no events have been lost so far.
            checkWriteError();
        }

        private void checkWriteError() throws Exception {
            Throwable error = this.writeError.getAndSet(null);
            if (error != null) {
                throw new IOException("Write failure", error);
            }
        }
    }

    /*
     * Pending transaction state snapshot representing combinations of transaction id, scope and stream
     */
    static class PendingTransaction implements Serializable {
        private final UUID uuid;
        private final String scope;
        private final String stream;
        private final Long watermark;

        public PendingTransaction(UUID uuid, String scope, String stream, Long watermark) {
            Preconditions.checkNotNull(uuid, "UUID");
            Preconditions.checkNotNull(scope, "scope");
            Preconditions.checkNotNull(stream, "stream");
            this.uuid = uuid;
            this.scope = scope;
            this.stream = stream;
            this.watermark = watermark;
        }

        public UUID getUuid() {
            return uuid;
        }

        public String getScope() {
            return scope;
        }

        public String getStream() {
            return stream;
        }

        public Long getWatermark() {
            return watermark;
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
