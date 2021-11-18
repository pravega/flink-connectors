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
import io.pravega.client.ClientConfig;
import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.connectors.flink.PravegaWriterMode;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.Committer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * This committer handles the final commit stage for EXACTLY_ONCE writer mode.
 * Internal writers are reconstructed and new transactions are resumed with
 * the Flink preserved committables that contains previous opened transactions.
 *
 * @param <T> The type of the event to be written.
 */
public class PravegaCommitter<T> implements Committer<PravegaTransactionState> {
    // --------- configurations for creating a FlinkPravegaInternalWriter ---------
    protected final ClientConfig clientConfig;  // The Pravega client config.
    protected final long txnLeaseRenewalPeriod;
    // The destination stream.
    @SuppressFBWarnings("SE_BAD_FIELD")
    protected final Stream stream;
    // The sink's mode of operation. This is used to provide different guarantees for the written events.
    protected final PravegaWriterMode writerMode;
    protected final SerializationSchema<T> serializationSchema;
    // The router used to partition events within a stream, can be null for random routing
    @Nullable
    protected final PravegaEventRouter<T> eventRouter;
    // --------- configurations for creating a FlinkPravegaInternalWriter ---------

    /**
     * A Pravega Committer that implements {@link Committer}.
     *
     * @param clientConfig          The Pravega client configuration.
     * @param stream                The destination stream.
     * @param txnLeaseRenewalPeriod Transaction lease renewal period in milliseconds.
     * @param writerMode            The Pravega writer mode.
     * @param serializationSchema   The implementation for serializing every event into pravega's storage format.
     * @param eventRouter           The implementation to extract the partition key from the event.
     */
    public PravegaCommitter(ClientConfig clientConfig,
                            Stream stream,
                            long txnLeaseRenewalPeriod,
                            PravegaWriterMode writerMode,
                            SerializationSchema<T> serializationSchema, PravegaEventRouter<T> eventRouter) {
        this.clientConfig = clientConfig;
        this.txnLeaseRenewalPeriod = txnLeaseRenewalPeriod;
        this.stream = stream;
        this.writerMode = writerMode;
        this.serializationSchema = serializationSchema;
        this.eventRouter = eventRouter;
    }

    @Override
    public List<PravegaTransactionState> commit(List<PravegaTransactionState> committables) throws IOException {
        committables.forEach(transactionState -> {
            FlinkPravegaInternalWriter<T> writer = new FlinkPravegaInternalWriter<>(
                    clientConfig, stream, txnLeaseRenewalPeriod, writerMode,
                    serializationSchema, eventRouter);
            writer.resumeTransaction(transactionState);
            writer.commitTransaction();
        });
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        // do nothing
    }
}
