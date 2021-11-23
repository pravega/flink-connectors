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
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.connectors.flink.PravegaWriterMode;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.Committer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * This committer handles the final commit stage for EXACTLY_ONCE writer mode.
 * Internal writers are reconstructed and new transactions are resumed with
 * the Flink preserved committables that contains previous opened transactions.
 *
 * @param <T> The type of the event to be written.
 */
public class PravegaCommitter<T> implements Committer<PravegaTransactionState> {
    private static final Logger LOG = LoggerFactory.getLogger(PravegaCommitter.class);

    // --------- configurations for creating a TransactionalEventStreamWriter ---------
    protected final ClientConfig clientConfig;  // The Pravega client config.
    protected final long txnLeaseRenewalPeriod;
    // The destination stream.
    @SuppressFBWarnings("SE_BAD_FIELD")
    protected final Stream stream;
    protected final SerializationSchema<T> serializationSchema;
    // The router used to partition events within a stream, can be null for random routing
    @Nullable
    protected final PravegaEventRouter<T> eventRouter;
    // --------- configurations for creating a TransactionalEventStreamWriter ---------

    @VisibleForTesting
    protected TransactionalEventStreamWriter<T> transactionalWriter;

    /**
     * A Pravega Committer that implements {@link Committer}.
     *
     * @param clientConfig          The Pravega client configuration.
     * @param stream                The destination stream.
     * @param txnLeaseRenewalPeriod Transaction lease renewal period in milliseconds.
     * @param serializationSchema   The implementation for serializing every event into pravega's storage format.
     * @param eventRouter           The implementation to extract the partition key from the event.
     */
    public PravegaCommitter(ClientConfig clientConfig,
                            Stream stream,
                            long txnLeaseRenewalPeriod,
                            SerializationSchema<T> serializationSchema, PravegaEventRouter<T> eventRouter) {
        this.clientConfig = clientConfig;
        this.txnLeaseRenewalPeriod = txnLeaseRenewalPeriod;
        this.stream = stream;
        this.serializationSchema = serializationSchema;
        this.eventRouter = eventRouter;
        this.transactionalWriter = initializeInternalWriter();
    }

    @Override
    public List<PravegaTransactionState> commit(List<PravegaTransactionState> committables) throws IOException {
        final String writerId = "";

        committables.forEach(transactionState -> {
            // TransactionalEventStreamWriter<T> transactionalWriter = initializeInternalWriter();
            Transaction<T> transaction = transactionalWriter
                    .getTxn(UUID.fromString(transactionState.getTransactionId()));
            LOG.info("{} - Transaction resumed with id {}.", writerId, transaction.getTxnId());

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
        });

        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        // do nothing
    }

    @VisibleForTesting
    protected TransactionalEventStreamWriter<T> initializeInternalWriter() {
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(stream.getScope(), clientConfig);
        Serializer<T> eventSerializer = new PravegaWriter.FlinkSerializer<>(serializationSchema);
        EventWriterConfig writerConfig = EventWriterConfig.builder()
                .transactionTimeoutTime(txnLeaseRenewalPeriod)
                .build();
        return clientFactory.createTransactionalEventWriter(stream.getStreamName(), eventSerializer, writerConfig);
    }
}
