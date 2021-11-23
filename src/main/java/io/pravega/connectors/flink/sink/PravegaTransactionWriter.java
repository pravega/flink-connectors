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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class PravegaTransactionWriter<T> implements SinkWriter<T, PravegaTransactionState, Void> {
    private static final Logger LOG = LoggerFactory.getLogger(PravegaTransactionWriter.class);

    protected final String writerId = UUID.randomUUID() + "";

    // The Pravega client config.
    @VisibleForTesting
    protected ClientConfig clientConfig;

    // Various timeouts
    @VisibleForTesting
    protected long txnLeaseRenewalPeriod;

    // The destination stream.
    @VisibleForTesting
    @SuppressFBWarnings("SE_BAD_FIELD")
    protected Stream stream;

    @VisibleForTesting
    protected SerializationSchema<T> serializationSchema;

    // The router used to partition events within a stream, can be null for random routing
    @Nullable
    @VisibleForTesting
    protected PravegaEventRouter<T> eventRouter;

    // Transactional Pravega writer instance
    @VisibleForTesting
    protected transient TransactionalEventStreamWriter<T> transactionalWriter;

    // Client factory for PravegaWriter instances
    @VisibleForTesting
    protected transient EventStreamClientFactory clientFactory = null;

    // Transaction
    @Nullable
    private transient Transaction<T> transaction = null;

    public PravegaTransactionWriter(Sink.InitContext context,
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

        beginTransaction();

        LOG.info("Initialized Pravega writer {} for stream: {} with controller URI: {}",
                writerId, stream, clientConfig.getControllerURI());
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

    public void beginTransaction() {
        transaction = transactionalWriter.beginTxn();
        LOG.info("{} - Transaction began with id {}.", writerId, transaction.getTxnId());
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
    public List<PravegaTransactionState> prepareCommit(boolean flush) throws IOException, InterruptedException {
        final List<PravegaTransactionState> transactionStates;
        try {
            flush();

            transactionStates = Collections.singletonList(PravegaTransactionState.of(this));

            beginTransaction();
        } catch (TxnFailedException e) {
            throw new IOException("", e);
        }
        LOG.info("Committing {} committables, final commit={}.", transactionStates, flush);
        return transactionStates;
    }

    public void abortTransaction() throws UnsupportedOperationException, AssertionError {
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

    public void flush() throws TxnFailedException, AssertionError {
        assert transaction != null;

        transaction.flush();
        LOG.info("{} - Flushed the transaction with id: {}", writerId, transaction.getTxnId());
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

    public String getTransactionId() {
        assert transaction != null;
        return transaction.getTxnId().toString();
    }
}
