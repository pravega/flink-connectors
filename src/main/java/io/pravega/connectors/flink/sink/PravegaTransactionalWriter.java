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
import io.pravega.client.EventStreamClientFactory;
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
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * A Pravega {@link TwoPhaseCommittingSink.PrecommittingSinkWriter} implementation that is suitable
 * for the {@link PravegaWriterMode#EXACTLY_ONCE} mode.
 *
 * <p>Note that the transaction is committed in a reconstructed one from the {@link PravegaCommitter} and
 * this writer only deals with the {@link PravegaTransactionalWriter#beginTransaction},
 * {@link PravegaTransactionalWriter#write}, and {@link PravegaTransactionalWriter#prepareCommit} stage.
 *
 * @param <T> The type of the event to be written.
 */
public class PravegaTransactionalWriter<T>
        implements TwoPhaseCommittingSink.PrecommittingSinkWriter<T, PravegaTransactionState> {

    private static final Logger LOG = LoggerFactory.getLogger(PravegaTransactionalWriter.class);

    // Client factory for PravegaTransactionWriter instances
    @VisibleForTesting
    protected transient EventStreamClientFactory clientFactory;

    // The Pravega client config.
    private final ClientConfig clientConfig;

    // Various timeouts
    private final long txnLeaseRenewalPeriod;

    // The destination stream.
    @SuppressFBWarnings("SE_BAD_FIELD")
    private final Stream stream;

    // The supplied event serializer.
    private final SerializationSchema<T> serializationSchema;

    // The router used to partition events within a stream, can be null for random routing
    @Nullable
    private final PravegaEventRouter<T> eventRouter;

    // Transactional Pravega writer instance
    private final transient TransactionalEventStreamWriter<T> transactionalWriter;

    // The writer id
    private final String writerId;

    // Transaction
    @Nullable
    private transient Transaction<T> transaction;

    private final SinkWriterMetricGroup metricGroup;

    /**
     * A Pravega writer that handles {@link PravegaWriterMode#EXACTLY_ONCE} writer mode.
     *
     * @param context               Some runtime info from sink.
     * @param clientConfig          The Pravega client configuration.
     * @param stream                The destination stream.
     * @param txnLeaseRenewalPeriod Transaction lease renewal period in milliseconds.
     * @param serializationSchema   The implementation for serializing every event into pravega's storage format.
     * @param eventRouter           The implementation to extract the partition key from the event.
     */
    public PravegaTransactionalWriter(Sink.InitContext context,
                                      ClientConfig clientConfig,
                                      Stream stream,
                                      long txnLeaseRenewalPeriod,
                                      SerializationSchema<T> serializationSchema,
                                      PravegaEventRouter<T> eventRouter) {
        this.clientConfig = clientConfig;
        this.stream = stream;
        this.txnLeaseRenewalPeriod = txnLeaseRenewalPeriod;
        this.serializationSchema = serializationSchema;
        this.eventRouter = eventRouter;
        this.transactionalWriter = initializeInternalWriter();
        this.writerId = UUID.randomUUID() + "-" + context.getSubtaskId();
        this.metricGroup = context.metricGroup();

        LOG.info("Initialized Pravega writer {} for stream: {} with controller URI: {}",
                writerId, stream, clientConfig.getControllerURI());

        this.transaction = beginTransaction();

        initFlinkMetrics();
    }

    @VisibleForTesting
    protected TransactionalEventStreamWriter<T> initializeInternalWriter() {
        clientFactory = EventStreamClientFactory.withScope(stream.getScope(), clientConfig);
        Serializer<T> eventSerializer = new FlinkSerializer<>(serializationSchema);
        EventWriterConfig writerConfig = EventWriterConfig.builder()
                .transactionTimeoutTime(txnLeaseRenewalPeriod)
                .build();
        return clientFactory.createTransactionalEventWriter(stream.getStreamName(), eventSerializer, writerConfig);
    }

    private Transaction<T> beginTransaction() {
        Transaction<T> transaction = transactionalWriter.beginTxn();
        LOG.info("{} - Transaction began with id {}.", writerId, transaction.getTxnId());
        return transaction;
    }

    @Override
    public Collection<PravegaTransactionState> prepareCommit() throws IOException, InterruptedException {
        final List<PravegaTransactionState> transactionStates;
        flush(true);

        transactionStates = Collections.singletonList(PravegaTransactionState.of(this));
        LOG.info("Committing {} committables.", transactionStates);

        transaction = beginTransaction();

        return transactionStates;
    }

    @Override
    public void write(T element, Context context) throws IOException, InterruptedException {
        try {
            assert transaction != null;

            if (eventRouter != null) {
                transaction.writeEvent(eventRouter.getRoutingKey(element), element);
            } else {
                transaction.writeEvent(element);
            }
        } catch (TxnFailedException | AssertionError e) {
            throw new IOException(e);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (endOfInput) {
            try {
                assert transaction != null;
                transaction.flush();
                LOG.info("{} - Flushed the transaction with id: {}", writerId, transaction.getTxnId());
            } catch (TxnFailedException | AssertionError e) {
                throw new IOException(e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("{} - Close the PravegaTransactionWriter with transaction {}",
                writerId, transaction == null ? null : transaction.getTxnId());

        Exception exception = null;

        if (transaction != null) {
            try {
                abortTransaction();
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

    private void abortTransaction() throws UnsupportedOperationException, AssertionError {
        assert transaction != null;

        final Transaction.Status status = transaction.checkStatus();
        if (status == Transaction.Status.OPEN) {
            transaction.abort();
            LOG.info("{} - Aborted the transaction: {}", writerId, transaction.getTxnId());
        } else {
            LOG.warn("{} - Transaction {} has unexpected transaction status {} while aborting",
                    writerId, transaction.getTxnId(), status);
        }
        transaction = null;
    }

    public String getTransactionId() {
        assert transaction != null;
        return transaction.getTxnId().toString();
    }

    private void initFlinkMetrics() {
        metricGroup.setCurrentSendTimeGauge(this::computeSendTime);
        registerMetricSync();
    }

    private long computeSendTime() {

    }

    private void registerMetricSync() {

    }

    @VisibleForTesting
    @Nullable
    protected PravegaEventRouter<T> getEventRouter() {
        return eventRouter;
    }

    @VisibleForTesting
    protected TransactionalEventStreamWriter<T> getInternalWriter() {
        return transactionalWriter;
    }
}
