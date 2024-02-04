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

import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.connectors.flink.utils.PravegaTestEnvironment;
import io.pravega.connectors.flink.utils.runtime.PravegaRuntime;
import io.pravega.connectors.flink.utils.runtime.PravegaRuntimeOperator;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.OptionalLong;

import static io.pravega.connectors.flink.sink.PravegaSinkBuilder.DEFAULT_TXN_LEASE_RENEWAL_PERIOD_MILLIS;
import static org.assertj.core.api.Assertions.assertThat;

public class PravegaSinkWriterITCase {
    private static final PravegaTestEnvironment PRAVEGA = new PravegaTestEnvironment(PravegaRuntime.container());
    private static final SinkWriter.Context SINK_WRITER_CONTEXT = new DummySinkWriterContext();

    private MetricListener metricListener;

    @BeforeAll
    public static void setupPravega() throws Exception {
        PRAVEGA.startUp();
    }

    @AfterAll
    public static void tearDownPravega() throws Exception {
        PRAVEGA.tearDown();
    }

    @BeforeEach
    public void setUp() {
        metricListener = new MetricListener();
    }

    @Test
    public void testTransactionWriterIncreasingRecordCounter() throws Exception {
        final OperatorIOMetricGroup operatorIOMetricGroup =
                UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup().getIOMetricGroup();
        final InternalSinkWriterMetricGroup metricGroup =
                InternalSinkWriterMetricGroup.mock(
                        metricListener.getMetricGroup(), operatorIOMetricGroup);
        final String stream = RandomStringUtils.randomAlphabetic(20);
        PRAVEGA.operator().createTestStream(stream, 1);
        try (final PravegaTransactionalWriter<Integer> writer =
                     (PravegaTransactionalWriter<Integer>) createWriter(PRAVEGA.operator(), metricGroup,
                             DeliveryGuarantee.EXACTLY_ONCE, PRAVEGA.operator().getStream(stream))) {
            final Counter numRecordsOut = metricGroup.getIOMetricGroup().getNumRecordsOutCounter();
            assertThat(numRecordsOut.getCount()).isEqualTo(0);

            writer.write(1, SINK_WRITER_CONTEXT);
            assertThat(numRecordsOut.getCount()).isEqualTo(1);
        }
    }

    @Test
    public void testEventWriterIncreasingRecordCounter() throws Exception {
        final OperatorIOMetricGroup operatorIOMetricGroup =
                UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup().getIOMetricGroup();
        final InternalSinkWriterMetricGroup metricGroup =
                InternalSinkWriterMetricGroup.mock(
                        metricListener.getMetricGroup(), operatorIOMetricGroup);
        final String stream = RandomStringUtils.randomAlphabetic(20);
        PRAVEGA.operator().createTestStream(stream, 1);
        try (final PravegaEventWriter<Integer> writer =
                     (PravegaEventWriter<Integer>) createWriter(PRAVEGA.operator(), metricGroup,
                             DeliveryGuarantee.AT_LEAST_ONCE, PRAVEGA.operator().getStream(stream))) {
            final Counter numRecordsOut = metricGroup.getIOMetricGroup().getNumRecordsOutCounter();
            final Counter numRecordsOutErrors = metricGroup.getNumRecordsOutErrorsCounter();
            assertThat(numRecordsOut.getCount()).isEqualTo(0);
            assertThat(numRecordsOutErrors.getCount()).isEqualTo(0);

            writer.write(1, SINK_WRITER_CONTEXT);
            assertThat(numRecordsOut.getCount()).isEqualTo(1);
            assertThat(numRecordsOutErrors.getCount()).isEqualTo(0);
        }
    }

    private SinkWriter<Integer> createWriter(
            PravegaRuntimeOperator operator,
            SinkWriterMetricGroup sinkWriterMetricGroup,
            DeliveryGuarantee guarantee,
            Stream stream) {
        PravegaEventRouter<Integer> router = event -> "fixedkey";

        if (guarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            return new PravegaTransactionalWriter<>(
                    new SinkInitContext(sinkWriterMetricGroup),
                    operator.getClientConfig(),
                    stream,
                    Time.milliseconds(DEFAULT_TXN_LEASE_RENEWAL_PERIOD_MILLIS).toMilliseconds(),
                    new IntSerializer(),
                    router);
        } else if (guarantee == DeliveryGuarantee.AT_LEAST_ONCE || guarantee == DeliveryGuarantee.NONE) {
            return new PravegaEventWriter<>(
                    new SinkInitContext(sinkWriterMetricGroup),
                    operator.getClientConfig(),
                    stream,
                    guarantee,
                    new IntSerializer(),
                    router);
        } else {
            throw new IllegalStateException("Failed to build Pravega sink writer with unknown write mode: " + guarantee);
        }
    }

    private static class SinkInitContext implements Sink.InitContext {

        private final SinkWriterMetricGroup metricGroup;

        SinkInitContext(
                SinkWriterMetricGroup metricGroup) {
            this.metricGroup = metricGroup;
        }

        @Override
        public JobID getJobId() {
            return null;
        }

        @Override
        public <T> TypeSerializer<T> createInputSerializer() {
            return null;
        }

        @Override
        public boolean isObjectReuseEnabled() {
            return false;
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public MailboxExecutor getMailboxExecutor() {
            return new SyncMailboxExecutor();
        }

        @Override
        public ProcessingTimeService getProcessingTimeService() {
            return null;
        }

        @Override
        public int getSubtaskId() {
            return 0;
        }

        @Override
        public int getNumberOfParallelSubtasks() {
            return 1;
        }

        @Override
        public int getAttemptNumber() {
            return 0;
        }

        @Override
        public SinkWriterMetricGroup metricGroup() {
            return metricGroup;
        }

        @Override
        public OptionalLong getRestoredCheckpointId() {
            return OptionalLong.empty();
        }

        @Override
        public SerializationSchema.InitializationContext
        asSerializationSchemaInitializationContext() {
            return null;
        }
    }

    private static class IntSerializer implements SerializationSchema<Integer> {
        @Override
        public byte[] serialize(Integer integer) {
            return ByteBuffer.allocate(4).putInt(0, integer).array();
        }
    }

    private static class DummySinkWriterContext implements SinkWriter.Context {
        @Override
        public long currentWatermark() {
            return 0;
        }

        @Override
        public Long timestamp() {
            return null;
        }
    }
}
