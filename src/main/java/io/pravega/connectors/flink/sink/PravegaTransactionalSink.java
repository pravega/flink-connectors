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

import io.pravega.client.ClientConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.Transaction;
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.connectors.flink.PravegaWriterMode;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * A Pravega sink for {@link PravegaWriterMode#EXACTLY_ONCE} writer mode.
 *
 * <p>Use {@link PravegaSinkBuilder} to construct a {@link PravegaTransactionalSink}.
 *
 * <p>{@link PravegaTransactionalSink} has two main components, {@link PravegaTransactionalWriter}
 * and {@link PravegaCommitter}.
 *
 * <p>The transactional writer will create transaction on initialization and after each checkpoint.
 * For every event, it will call {@link Transaction#writeEvent}. {@link Transaction#flush} will be
 * called before next checkpoint to make sure all events are acknowledged by Pravega.
 * Note it will not commit transactions but let {@link PravegaCommitter} to do this.
 *
 * <p>The committer will use the transactions provided by the transactional writer and commit them.
 * If there is an error during commit, it will log the error and throw the failed transaction away.
 *
 * @param <T> The type of the event to be written.
 */
@Experimental
public class PravegaTransactionalSink<T>
        extends PravegaSink<T>
        implements TwoPhaseCommittingSink<T, PravegaTransactionState> {
    // Transaction timeouts
    private final long txnLeaseRenewalPeriod;

    /**
     * Creates a new Pravega Transactional Sink instance which can be added as a sink to a Flink job.
     * It will create a {@link PravegaTransactionalWriter} on demand with following parameters.
     * We can use {@link PravegaSinkBuilder} to build such a sink.
     *
     * @param clientConfig          The Pravega client configuration.
     * @param stream                The destination stream.
     * @param txnLeaseRenewalPeriod THe transaction timeout after any operations.
     * @param serializationSchema   The implementation for serializing every event into pravega's storage format.
     * @param eventRouter           The implementation to extract the partition key from the event.
     */
    PravegaTransactionalSink(ClientConfig clientConfig, Stream stream, long txnLeaseRenewalPeriod,
                             SerializationSchema<T> serializationSchema, PravegaEventRouter<T> eventRouter) {
        super(clientConfig, stream, serializationSchema, eventRouter);
        Preconditions.checkArgument(txnLeaseRenewalPeriod > 0, "txnLeaseRenewalPeriod must be > 0");
        this.txnLeaseRenewalPeriod = txnLeaseRenewalPeriod;
    }

    @Override
    public TwoPhaseCommittingSink.PrecommittingSinkWriter<T, PravegaTransactionState>
    createWriter(InitContext context) throws IOException {
        return new PravegaTransactionalWriter<>(
                context,
                clientConfig,
                stream,
                txnLeaseRenewalPeriod,
                serializationSchema,
                eventRouter);
    }

    @Override
    public Committer<PravegaTransactionState> createCommitter() throws IOException {
        return new PravegaCommitter<>(clientConfig, stream, txnLeaseRenewalPeriod, serializationSchema);
    }

    @Override
    public SimpleVersionedSerializer<PravegaTransactionState> getCommittableSerializer() {
        return new PravegaTransactionStateSerializer();
    }
}
