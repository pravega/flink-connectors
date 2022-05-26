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
import io.pravega.connectors.flink.PravegaEventRouter;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.io.IOException;

@Experimental
public class PravegaTransactionalSink<T> implements TwoPhaseCommittingSink<T, PravegaTransactionState> {
    private static final String PRAVEGA_WRITER_METRICS_GROUP = "PravegaWriter";
    private static final String SCOPED_STREAM_METRICS_GAUGE = "stream";

    // flag to enable/disable metrics
    private final boolean enableMetrics;

    // The Pravega client config.
    private final ClientConfig clientConfig;

    // The destination stream.
    private final Stream stream;

    // Various timeouts
    private final long txnLeaseRenewalPeriod;

    // The supplied event serializer.
    private final SerializationSchema<T> serializationSchema;

    // The router used to partition events within a stream, can be null for random routing
    @Nullable
    private final PravegaEventRouter<T> eventRouter;

    PravegaTransactionalSink(boolean enableMetrics, ClientConfig clientConfig,
                             Stream stream, long txnLeaseRenewalPeriod,
                             SerializationSchema<T> serializationSchema, PravegaEventRouter<T> eventRouter) {
        this.enableMetrics = enableMetrics;
        this.clientConfig = Preconditions.checkNotNull(clientConfig, "clientConfig");
        this.stream = Preconditions.checkNotNull(stream, "stream");
        Preconditions.checkArgument(txnLeaseRenewalPeriod > 0, "txnLeaseRenewalPeriod must be > 0");
        this.txnLeaseRenewalPeriod = txnLeaseRenewalPeriod;
        this.serializationSchema = Preconditions.checkNotNull(serializationSchema, "serializationSchema");
        this.eventRouter = eventRouter;
    }

    @Override
    public TwoPhaseCommittingSink.PrecommittingSinkWriter<T, PravegaTransactionState>
    createWriter(InitContext context) throws IOException {
        if (enableMetrics) {
            MetricGroup pravegaWriterMetricGroup = context.metricGroup().addGroup(PRAVEGA_WRITER_METRICS_GROUP);
            pravegaWriterMetricGroup.gauge(SCOPED_STREAM_METRICS_GAUGE, new StreamNameGauge(stream.getScopedName()));
        }

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
