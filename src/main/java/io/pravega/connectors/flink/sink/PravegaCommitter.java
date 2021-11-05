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
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.Committer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
public class PravegaCommitter<T> implements Committer<PravegaTransactionState> {
    // The Pravega client config.
    private final ClientConfig clientConfig;

    // Various timeouts
    private final long txnLeaseRenewalPeriod;

    // The destination stream.
    @SuppressFBWarnings("SE_BAD_FIELD")
    private final Stream stream;

    // The sink's mode of operation. This is used to provide different guarantees for the written events.
    private final PravegaWriterMode writerMode;

    // flag to enable/disable watermark
    private final boolean enableWatermark;

    private final SerializationSchema<T> serializationSchema;

    // The router used to partition events within a stream, can be null for random routing
    @Nullable
    private final PravegaEventRouter<T> eventRouter;

    private final List<FlinkPravegaInternalWriter<T>> recoveryWriters = new ArrayList<>();

    public PravegaCommitter(ClientConfig clientConfig,
                            long txnLeaseRenewalPeriod,
                            Stream stream,
                            PravegaWriterMode writerMode,
                            boolean enableWatermark,
                            SerializationSchema<T> serializationSchema, PravegaEventRouter<T> eventRouter) {
        this.clientConfig = clientConfig;
        this.txnLeaseRenewalPeriod = txnLeaseRenewalPeriod;
        this.stream = stream;
        this.writerMode = writerMode;
        this.enableWatermark = enableWatermark;
        this.serializationSchema = serializationSchema;
        this.eventRouter = eventRouter;
    }

    @Override
    public List<PravegaTransactionState> commit(List<PravegaTransactionState> committables) throws IOException {
        committables.forEach(transaction -> {
            FlinkPravegaInternalWriter<T> writer = new FlinkPravegaInternalWriter<>(
                    clientConfig, stream, txnLeaseRenewalPeriod, writerMode, enableWatermark,
                    serializationSchema, eventRouter, transaction.getWriterId(),
                    transaction.getWatermark(), transaction.getTransactionId());
            writer.commitTransaction();

            // temporarily store the writer so that if we need to abort
            // in case of failure, close it.
            recoveryWriters.add(writer);
        });
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        try {
            for (FlinkPravegaInternalWriter<T> writer : recoveryWriters) {
                writer.close();
            }
        } finally {
            recoveryWriters.clear();
        }
    }
}
