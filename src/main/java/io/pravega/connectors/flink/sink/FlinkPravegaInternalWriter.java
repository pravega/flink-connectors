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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.connectors.flink.PravegaWriterMode;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A customized Pravega writer that handles the actual writer call and
 * the different {@link PravegaWriterMode}.
 *
 * @param <T> The type of the event to be written.
 */
public class FlinkPravegaInternalWriter<T> implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkPravegaInternalWriter.class);

    // ----------- Runtime fields ----------------

    // Error which will be detected asynchronously and reported to Flink
    @VisibleForTesting
    volatile AtomicReference<Throwable> writeError = new AtomicReference<>(null);

    // Used to track confirmation from all writes to ensure guaranteed writes.
    @VisibleForTesting
    AtomicLong pendingWritesCount = new AtomicLong();

    private final String writerId = UUID.randomUUID() + "";

    private transient ExecutorService executorService;

    // ----------- configuration fields -----------

    // The Pravega client config.
    private final ClientConfig clientConfig;

    // Various timeouts
    private final long txnLeaseRenewalPeriod;

    // The destination stream.
    @SuppressFBWarnings("SE_BAD_FIELD")
    private final Stream stream;

    // The sink's mode of operation. This is used to provide different guarantees for the written events.
    private final PravegaWriterMode writerMode;

    private final SerializationSchema<T> serializationSchema;

    // The router used to partition events within a stream, can be null for random routing
    @Nullable
    private final PravegaEventRouter<T> eventRouter;

    // Pravega writer instance
    @Nullable
    private transient EventStreamWriter<T> writer = null;

    // Transactional Pravega writer instance
    @Nullable
    private transient TransactionalEventStreamWriter<T> transactionalWriter = null;

    // Transaction
    @Nullable
    private transient Transaction<T> transaction = null;

    // Client factory for PravegaWriter instances
    @Nullable
    private transient EventStreamClientFactory clientFactory = null;

    /**
     * An internal writer that handles the actual writing process.
     *
     * @param clientConfig          The Pravega client configuration.
     * @param stream                The destination stream.
     * @param txnLeaseRenewalPeriod Transaction lease renewal period in milliseconds.
     * @param writerMode            The Pravega writer mode.
     * @param serializationSchema   The implementation for serializing every event into pravega's storage format.
     * @param eventRouter           The implementation to extract the partition key from the event.
     */
    public FlinkPravegaInternalWriter(ClientConfig clientConfig,
                                      Stream stream,
                                      long txnLeaseRenewalPeriod,
                                      PravegaWriterMode writerMode,
                                      SerializationSchema<T> serializationSchema,
                                      PravegaEventRouter<T> eventRouter) {
        this.clientConfig = clientConfig;
        this.stream = stream;
        this.txnLeaseRenewalPeriod = txnLeaseRenewalPeriod;
        this.writerMode = writerMode;
        this.serializationSchema = serializationSchema;
        this.eventRouter = eventRouter;

        initializeInternalWriter();

        LOG.info("Initialized Pravega writer {} for stream: {} with controller URI: {}",
                writerId, stream, clientConfig.getControllerURI());
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
        createInternalWriter(this.clientFactory);
    }

    private void createInternalWriter(EventStreamClientFactory clientFactory) {
        Serializer<T> eventSerializer = new PravegaWriter.FlinkSerializer<>(serializationSchema);
        EventWriterConfig writerConfig = EventWriterConfig.builder()
                .transactionTimeoutTime(txnLeaseRenewalPeriod)
                .build();
        if (this.writerMode == PravegaWriterMode.EXACTLY_ONCE) {
            transactionalWriter = clientFactory.createTransactionalEventWriter(stream.getStreamName(), eventSerializer, writerConfig);
        } else {
            executorService = createExecutorService();
            writer = clientFactory.createEventWriter(stream.getStreamName(), eventSerializer, writerConfig);
        }
    }

    private boolean isCheckpointEnabled() {
        return true;
        // return ((StreamingRuntimeContext) getRuntimeContext()).isCheckpointingEnabled();
    }

    public void beginTransaction() {
        assert writerMode == PravegaWriterMode.EXACTLY_ONCE;

        assert transactionalWriter != null;
        transaction = transactionalWriter.beginTxn();

        LOG.info("{} - Transaction began with id {}.", writerId, transaction.getTxnId());
    }

    public void resumeTransaction(PravegaTransactionState transactionState) {
        assert writerMode == PravegaWriterMode.EXACTLY_ONCE && transactionalWriter != null;

        transaction = transactionalWriter.getTxn(UUID.fromString(transactionState.getTransactionId()));

        LOG.info("{} - Transaction resumed with id {}.", writerId, transaction.getTxnId());
    }

    public void write(T element) throws TxnFailedException, IOException {
        checkWriteError();

        switch (writerMode) {
            case EXACTLY_ONCE:
                assert transaction != null;

                if (eventRouter != null) {
                    transaction.writeEvent(eventRouter.getRoutingKey(element), element);
                } else {
                    transaction.writeEvent(element);
                }

                break;
            case ATLEAST_ONCE:
            case BEST_EFFORT:
                assert writer != null;
                this.pendingWritesCount.incrementAndGet();
                final CompletableFuture<Void> future;
                if (eventRouter != null) {
                    future = writer.writeEvent(eventRouter.getRoutingKey(element), element);
                } else {
                    future = writer.writeEvent(element);
                }
                future.whenCompleteAsync(
                        (result, e) -> {
                            if (e != null) {
                                LOG.warn("Detected a write failure", e);

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

    public void commitTransaction() {
        assert writerMode == PravegaWriterMode.EXACTLY_ONCE && transaction != null;

        // This may come from a job recovery from a non-transactional writer.
        if (transaction.getTxnId().toString() == null) {
            return;
        }

        try {
            final Transaction.Status status = transaction.checkStatus();
            if (status == Transaction.Status.OPEN) {
                transaction.commit();
                LOG.debug("{} - Committed transaction {}.", writerId, transaction.getTxnId());
            } else {
                LOG.warn("{} - Transaction {} has unexpected transaction status {} while committing.",
                        writerId, transaction.getTxnId(), status);
            }
        } catch (TxnFailedException e) {
            LOG.error("{} - Transaction {} commit failed.", writerId, transaction.getTxnId());
        } catch (StatusRuntimeException e) {
            if (e.getStatus() == Status.NOT_FOUND) {
                LOG.error("{} - Transaction {} not found.", writerId, transaction.getTxnId());
            }
        }
        transaction = null;
    }

    public void abortTransaction() throws UnsupportedOperationException, AssertionError {
        switch (writerMode) {
            case EXACTLY_ONCE:
                if (transaction == null) {
                    break;
                }
                // This may come from a job recovery from a non-transactional writer.
                if (transaction.getTxnId() == null) {
                    break;
                }
                final Transaction.Status status = transaction.checkStatus();
                if (status == Transaction.Status.OPEN) {
                    transaction.abort();
                    LOG.info("{} - Aborted the transaction: {}", writerId, transaction.getTxnId());
                } else {
                    LOG.warn("{} - Transaction {} has unexpected transaction status {} while aborting",
                            writerId, transaction.getTxnId(), status);
                }
                transaction = null;
                break;
            case ATLEAST_ONCE:
            case BEST_EFFORT:
                break;
            default:
                throw new UnsupportedOperationException("Not implemented writer mode");
        }
    }

    @VisibleForTesting
    public void flushAndVerify() throws IOException, InterruptedException, TxnFailedException, AssertionError {
        switch (writerMode) {
            case EXACTLY_ONCE:
                assert transaction != null;
                LOG.info("{} - Flush txn id: {}", writerId, transaction.getTxnId());
                transaction.flush();
                break;
            case BEST_EFFORT:
            case ATLEAST_ONCE:
                assert writer != null;
                writer.flush();

                // Wait until all errors, if any, have been recorded.
                synchronized (this) {
                    while (this.pendingWritesCount.get() > 0) {
                        this.wait();
                    }
                }

                checkWriteError();
                break;
        }
    }

    private void checkWriteError() throws IOException {
        Throwable error = this.writeError.getAndSet(null);
        if (error != null) {
            throw new IOException("Write failure", error);
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("{} - Close the FlinkPravegaInternalWriter with transaction {}",
                writerId, transaction == null ? null : transaction.getTxnId());

        Exception exception = null;

        try {
            abortTransaction();
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

    public String getTransactionId() {
        assert writerMode == PravegaWriterMode.EXACTLY_ONCE && transaction != null;
        return transaction.getTxnId().toString();
    }

    @VisibleForTesting
    protected PravegaEventRouter<T> getEventRouter() {
        return eventRouter;
    }

    @VisibleForTesting
    protected PravegaWriterMode getPravegaWriterMode() {
        return writerMode;
    }

    @VisibleForTesting
    @Nullable
    protected EventStreamWriter<T> getWriter() {
        return writer;
    }

    @VisibleForTesting
    @Nullable
    protected TransactionalEventStreamWriter<T> getTransactionalWriter() {
        return transactionalWriter;
    }

    @VisibleForTesting
    protected EventStreamClientFactory createClientFactory(String scopeName, ClientConfig clientConfig) {
        return EventStreamClientFactory.withScope(scopeName, clientConfig);
    }

    @VisibleForTesting
    protected ExecutorService createExecutorService() {
        return Executors.newSingleThreadExecutor();
    }
}
