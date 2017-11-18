/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink;

import com.google.common.base.Preconditions;
import io.pravega.client.ClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Transaction;
import io.pravega.common.Exceptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Flink sink implementation for writing into pravega storage.
 *
 * @param <T> The type of the event to be written.
 */
@Slf4j
public class FlinkPravegaWriter<T>
        extends RichSinkFunction<T>
        implements ListCheckpointed<UUID>, CheckpointListener {

    private static final long serialVersionUID = 1L;

    // the numbers below are picked somewhat arbitrarily at this point

    private static final long DEFAULT_TXN_TIMEOUT_MILLIS = 2 * 60 * 60 * 1000; // 2 hours

    private static final long DEFAULT_TX_SCALE_GRACE_MILLIS = 10 * 60 * 1000; // 10 minutes

    // Writer interface to assist exactly-once and atleast-once functionality
    @VisibleForTesting
    transient AbstractInternalWriter writer = null;

    // ----------- configuration fields -----------

    // The supplied event serializer.
    private final SerializationSchema<T> serializationSchema;

    // The router used to partition events within a stream.
    private final PravegaEventRouter<T> eventRouter;

    // The pravega controller endpoint.
    private final URI controllerURI;

    // The scope name of the destination stream.
    private final String scopeName;

    // The pravega stream name to write events to.
    private final String streamName;

    private final long txnTimeoutMillis;
    private final long txnGracePeriodMillis;

    // The sink's mode of operation. This is used to provide different guarantees for the written events.
    private PravegaWriterMode writerMode = PravegaWriterMode.ATLEAST_ONCE;

    // Client factory for PravegaWriter instances
    private transient ClientFactory clientFactory = null;

    /**
     * The flink pravega writer instance which can be added as a sink to a flink job.
     *
     * @param controllerURI         The pravega controller endpoint address.
     * @param scope                 The destination stream's scope name.
     * @param streamName            The destination stream Name.
     * @param serializationSchema   The implementation for serializing every event into pravega's storage format.
     * @param router                The implementation to extract the partition key from the event.
     */
    public FlinkPravegaWriter(
            final URI controllerURI,
            final String scope,
            final String streamName,
            final SerializationSchema<T> serializationSchema,
            final PravegaEventRouter<T> router) {

        this(controllerURI, scope, streamName, serializationSchema, router,
                DEFAULT_TXN_TIMEOUT_MILLIS, DEFAULT_TX_SCALE_GRACE_MILLIS);
    }

    /**
     * The flink pravega writer instance which can be added as a sink to a flink job.
     *
     * @param controllerURI         The pravega controller endpoint address.
     * @param scope                 The destination stream's scope name.
     * @param streamName            The destination stream Name.
     * @param serializationSchema   The implementation for serializing every event into pravega's storage format.
     * @param router                The implementation to extract the partition key from the event.
     * @param txnTimeoutMillis      The number of milliseconds after which the transaction will be aborted.
     * @param txnGracePeriodMillis  The maximum amount of time, in milliseconds, until which transaction may
     *                              remain active, after a scale operation has been initiated on the underlying stream.
     */
    public FlinkPravegaWriter(
            final URI controllerURI,
            final String scope,
            final String streamName,
            final SerializationSchema<T> serializationSchema,
            final PravegaEventRouter<T> router,
            final long txnTimeoutMillis,
            final long txnGracePeriodMillis) {

        Preconditions.checkNotNull(controllerURI, "controllerURI");
        Preconditions.checkNotNull(scope, "scope");
        Preconditions.checkNotNull(streamName, "streamName");
        Preconditions.checkNotNull(serializationSchema, "serializationSchema");
        Preconditions.checkNotNull(router, "router");
        Preconditions.checkArgument(txnTimeoutMillis > 0, "txnTimeoutMillis must be > 0");
        Preconditions.checkArgument(txnGracePeriodMillis > 0, "txnGracePeriodMillis must be > 0");

        this.controllerURI = controllerURI;
        this.scopeName = scope;
        this.streamName = streamName;
        this.serializationSchema = serializationSchema;
        this.eventRouter = router;

        this.txnTimeoutMillis = txnTimeoutMillis;
        this.txnGracePeriodMillis = txnGracePeriodMillis;
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
     * Sets this writer's operating mode.
     *
     * @param writerMode    The mode of operation.
     */
    public void setPravegaWriterMode(PravegaWriterMode writerMode) {
        Preconditions.checkNotNull(writerMode);
        this.writerMode = writerMode;
    }

    // ------------------------------------------------------------------------

    @Override
    public void open(Configuration parameters) throws Exception {
        initializeInternalWriter();
        writer.open();
        log.info("Initialized Pravega writer for stream: {}/{} with controller URI: {}", scopeName,
                streamName, controllerURI);
    }

    @Override
    public void invoke(T event) throws Exception {
        writer.write(event);
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

    // ------------------------------------------------------------------------

    @Override
    public List<UUID> snapshotState(long checkpointId, long checkpointTime) throws Exception {
        return writer.snapshotState(checkpointId, checkpointTime);
    }

    /**
     * Restores the state, which here means the IDs of transaction for which we have
     * to ensure that they are really committed.
     *
     * Note: {@code restoreState} is called before {@code open}.
     */
    @Override
    public void restoreState(List<UUID> list) throws Exception {
        initializeInternalWriter();
        writer.restoreState(list);
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

    // ------------------------------------------------------------------------
    //  helper methods
    // ------------------------------------------------------------------------

    @VisibleForTesting
    protected ClientFactory createClientFactory(String scopeName, URI controllerURI) {
        return ClientFactory.withScope(scopeName, controllerURI);
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
        return Executors.newFixedThreadPool(5);
    }

    private void initializeInternalWriter() {
        if (this.writer != null) {
            return;
        }

        if (this.writerMode == PravegaWriterMode.EXACTLY_ONCE && !isCheckpointEnabled()) {
            // Pravega transaction writer (exactly-once) implementation can be used only when checkpoint is enabled
            throw new UnsupportedOperationException("Enable checkpointing to use the exactly-once writer mode.");
        }

        this.clientFactory = createClientFactory(scopeName, controllerURI);
        this.writer = createInternalWriter();
    }

    private boolean isCheckpointEnabled() {
        return ((StreamingRuntimeContext) getRuntimeContext()).isCheckpointingEnabled();
    }

    protected String name() {
        return getRuntimeContext().getTaskNameWithSubtasks();
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

        TransactionAndCheckpoint(Transaction<T> transaction, long checkpointId) {
            this.transaction = transaction;
            this.checkpointId = checkpointId;
        }

        Transaction<T> transaction() {
            return transaction;
        }

        long checkpointId() {
            return checkpointId;
        }

        @Override
        public String toString() {
            return "(checkpoint: " + checkpointId + ", transaction: " + transaction.getTxnId() + ')';
        }
    }

    /*
     * An abstract internal writer.
     */
    @VisibleForTesting
    abstract class AbstractInternalWriter {

        protected final EventStreamWriter<T> pravegaWriter;

        AbstractInternalWriter(ClientFactory clientFactory) {
            Serializer<T> eventSerializer = new FlinkSerializer<>(serializationSchema);
            EventWriterConfig writerConfig = EventWriterConfig.builder()
                    .transactionTimeoutTime(txnTimeoutMillis)
                    .transactionTimeoutScaleGracePeriod(txnGracePeriodMillis)
                    .build();
            pravegaWriter = clientFactory.createEventWriter(streamName, eventSerializer, writerConfig);
        }

        abstract void open() throws Exception;

        abstract void write(T event) throws Exception;

        void close() throws Exception {
            pravegaWriter.close();
        }

        abstract List<UUID> snapshotState(long checkpointId, long checkpointTime) throws Exception;

        abstract void restoreState(List<UUID> list) throws Exception;

        abstract void notifyCheckpointComplete(long checkpointId) throws Exception;

    }

    /*
     * Implements an {@link AbstractInternalWriter} using transactional writes to Pravega.
     */
    @VisibleForTesting
    class TransactionalWriter extends AbstractInternalWriter {

        /** The currently running transaction to which we write */
        @VisibleForTesting
        Transaction<T> currentTxn;

        /** The transactions that are complete from Flink's view (their checkpoint was triggered),
         * but not fully committed, because their corresponding checkpoint is not yet confirmed */
        @VisibleForTesting
        final ArrayDeque<TransactionAndCheckpoint<T>> txnsPendingCommit;

        TransactionalWriter(ClientFactory clientFactory) {
            super(clientFactory);
            this.txnsPendingCommit = new ArrayDeque<>();
        }

        @Override
        public void open() throws Exception {
            // start the transaction that will hold the elements till the first checkpoint
            this.currentTxn = pravegaWriter.beginTxn();
            log.debug("{} - started first transaction '{}'", name(), this.currentTxn.getTxnId());
        }

        @Override
        public void write(T event) throws Exception {
            this.currentTxn.writeEvent(eventRouter.getRoutingKey(event), event);
        }

        @Override
        public void close() throws Exception {
            Exception exception = null;

            Transaction<?> txn = this.currentTxn;
            if (txn != null) {
                try {
                    Exceptions.handleInterrupted( txn::abort );
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
        public List<UUID> snapshotState(long checkpointId, long checkpointTime) throws Exception {
            // this is like the pre-commit of a 2-phase-commit transaction
            // we are ready to commit and remember the transaction

            final Transaction<T> txn = this.currentTxn;
            Preconditions.checkState(txn != null, "bug: no transaction object when performing state snapshot");

            log.debug("{} - checkpoint {} triggered, flushing transaction '{}'", name(), checkpointId, txn.getTxnId());

            // make sure all events go out
            txn.flush();

            // remember the transaction to be committed when the checkpoint is confirmed
            this.txnsPendingCommit.addLast(new TransactionAndCheckpoint<>(txn, checkpointId));

            // start the next transaction for what comes after this checkpoint
            this.currentTxn = pravegaWriter.beginTxn();

            log.debug("{} - started new transaction '{}'", name(), this.currentTxn.getTxnId());
            log.debug("{} - storing pending transactions {}", name(), txnsPendingCommit);

            // store all pending transactions in the checkpoint state
            return txnsPendingCommit.stream().map( v -> v.transaction().getTxnId() ).collect(Collectors.toList());
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

                log.info("{} - checkpoint {} complete, committing completed checkpoint transaction {}",
                        name(), checkpointId, txn.transaction().getTxnId());

                // the big assumption is that this now actually works and that the transaction has not timed out, yet

                // TODO: currently, if this fails, there is actually data loss
                // see https://github.com/pravega/flink-connectors/issues/5
                txn.transaction().commit();

                log.debug("{} - committed checkpoint transaction {}", name(), txn.transaction().getTxnId());

            }
        }

        @Override
        public void restoreState(List<UUID> list) throws Exception {
            // when we are restored with a UUID, we don't really know whether the
            // transaction was already committed, or whether there was a failure between
            // completing the checkpoint on the master, and notifying the writer here.

            // (the common case is actually that is was already committed, the window
            // between the commit on the master and the notification here is very small)

            // it is possible to not have any transactions at all if there was a failure before
            // the first completed checkpoint, or in case of a scale-out event, where some of the
            // new task do not have and transactions assigned to check)

            // we can have more than one transaction to check in case of a scale-in event, or
            // for the reasons discussed in the 'notifyCheckpointComplete()' method.

            if (list != null && list.size() > 0) {
                for (UUID txnId : list) {
                    if (txnId != null) {
                        final Transaction<?> txn = pravegaWriter.getTxn(txnId);
                        final Transaction.Status status = txn.checkStatus();

                        if (status == Transaction.Status.OPEN) {
                            // that is the case when a crash happened between when the master committed
                            // the checkpoint, and the sink could be notified
                            log.info("{} - committing completed checkpoint transaction {} after task restore",
                                    name(), txnId);

                            txn.commit();

                            log.debug("{} - committed checkpoint transaction {}", name(), txnId);

                        } else if (status == Transaction.Status.COMMITTED || status == Transaction.Status.COMMITTING) {
                            // that the common case
                            log.debug("{} - at restore, transaction {} was already committed", name(), txnId);

                        } else {
                            log.warn("{} - found unexpected transaction status {} for transaction {} on task restore. " +
                                    "Transaction probably timed out between failure and restore. ", name(), status, txnId);
                        }
                    }
                }
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

        NonTransactionalWriter(ClientFactory clientFactory, ExecutorService executorService) {
            super(clientFactory);
            this.writeError = new AtomicReference<>(null);
            this.pendingWritesCount = new AtomicInteger(0);
            this.executorService = executorService;
        }

        @Override
        public void open() throws Exception {
        }

        @Override
        public void write(T event) throws Exception {

            checkWriteError();

            this.pendingWritesCount.incrementAndGet();
            final CompletableFuture<Void> future = pravegaWriter.writeEvent(eventRouter.getRoutingKey(event), event);
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
        public List<UUID> snapshotState(long checkpointId, long checkpointTime) throws Exception {
            log.debug("Snapshot triggered, wait for all pending writes to complete");
            flushAndVerify();
            return new ArrayList<>();
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            //do nothing
        }

        @Override
        public void restoreState(List<UUID> list) throws Exception {
            //do nothing
        }

        // Wait until all pending writes are completed and throw any errors detected.
        @VisibleForTesting
        void flushAndVerify() throws Exception {
            pravegaWriter.flush();

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

}