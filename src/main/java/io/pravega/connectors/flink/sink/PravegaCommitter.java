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
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.connectors.flink.PravegaWriterMode;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * This committer only works in {@link PravegaWriterMode#EXACTLY_ONCE} and
 * handles the final commit stage for the transaction. <p>
 * The transaction is resumed via {@link PravegaTransactionState#getTransactionId()}
 * which handles by the Flink sink mechanism.
 *
 * @param <T> The type of the event to be written.
 */
public class PravegaCommitter<T> implements Committer<PravegaTransactionState> {
    private static final Logger LOG = LoggerFactory.getLogger(PravegaCommitter.class);

    @VisibleForTesting
    protected transient EventStreamClientFactory clientFactory;
    @VisibleForTesting
    protected TransactionalEventStreamWriter<T> transactionalWriter;

    // --------- configurations for creating a TransactionalEventStreamWriter ---------
    private final ClientConfig clientConfig;
    private final long txnLeaseRenewalPeriod;
    @SuppressFBWarnings("SE_BAD_FIELD")
    private final Stream stream;
    private final SerializationSchema<T> serializationSchema;
    // --------- configurations for creating a TransactionalEventStreamWriter ---------

    /**
     * A Pravega Committer that implements {@link Committer}.
     *
     * @param clientConfig          The Pravega client configuration.
     * @param stream                The destination stream.
     * @param txnLeaseRenewalPeriod Transaction lease renewal period in milliseconds.
     * @param serializationSchema   The implementation for serializing every event into pravega's storage format.
     */
    public PravegaCommitter(ClientConfig clientConfig,
                            Stream stream,
                            long txnLeaseRenewalPeriod,
                            SerializationSchema<T> serializationSchema) {
        this.clientConfig = clientConfig;
        this.txnLeaseRenewalPeriod = txnLeaseRenewalPeriod;
        this.stream = stream;
        this.serializationSchema = serializationSchema;
        this.transactionalWriter = initializeInternalWriter();
    }

    @Override
    public List<PravegaTransactionState> commit(List<PravegaTransactionState> committables) throws IOException {
        committables.forEach(transactionState -> {
            Transaction<T> transaction = transactionalWriter
                    .getTxn(UUID.fromString(transactionState.getTransactionId()));
            LOG.info("Transaction resumed with id {}.", transaction.getTxnId());

            try {
                final Transaction.Status status = transaction.checkStatus();
                if (status == Transaction.Status.OPEN) {
                    transaction.commit();
                    LOG.info("Committed transaction {}.", transaction.getTxnId());
                } else {
                    LOG.warn("Transaction {} has unexpected transaction status {} while committing.",
                            transaction.getTxnId(), status);
                }
            } catch (TxnFailedException e) {
                LOG.error("Transaction {} commit failed.", transaction.getTxnId());
            } catch (StatusRuntimeException e) {
                if (e.getStatus() == Status.NOT_FOUND) {
                    LOG.error("Transaction {} not found.", transaction.getTxnId());
                }
            }
        });

        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        Exception exception = null;

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

    @VisibleForTesting
    protected TransactionalEventStreamWriter<T> initializeInternalWriter() {
        clientFactory = EventStreamClientFactory.withScope(stream.getScope(), clientConfig);
        Serializer<T> eventSerializer = new FlinkSerializer<>(serializationSchema);
        EventWriterConfig writerConfig = EventWriterConfig.builder()
                .transactionTimeoutTime(txnLeaseRenewalPeriod)
                .build();
        return clientFactory.createTransactionalEventWriter(stream.getStreamName(), eventSerializer, writerConfig);
    }
}
