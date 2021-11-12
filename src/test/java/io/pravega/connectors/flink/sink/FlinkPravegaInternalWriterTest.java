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
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.connectors.flink.PravegaWriterMode;
import io.pravega.connectors.flink.utils.DirectExecutorService;
import io.pravega.connectors.flink.utils.IntegerSerializationSchema;
import io.pravega.connectors.flink.utils.StreamSinkOperatorTestHarness;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.streaming.runtime.operators.sink.SinkOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static io.pravega.connectors.flink.AbstractStreamingWriterBuilder.DEFAULT_TXN_LEASE_RENEWAL_PERIOD_MILLIS;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FlinkPravegaInternalWriterTest {
    private static final ClientConfig MOCK_CLIENT_CONFIG = ClientConfig.builder().build();
    private static final String MOCK_SCOPE_NAME = "scope";
    private static final String MOCK_STREAM_NAME = "stream";
    private static final String ROUTING_KEY = "fixed";
    private static final PravegaEventRouter<Integer> FIXED_EVENT_ROUTER = event -> ROUTING_KEY;

    @Test
    public void testConstructor() {
        EventStreamWriter<Integer> pravegaWriter = mockEventStreamWriter();
        PravegaEventRouter<Integer> eventRouter = FIXED_EVENT_ROUTER;
        PravegaWriterMode writerMode = PravegaWriterMode.ATLEAST_ONCE;
        FlinkPravegaInternalWriter<Integer> writer = spyInternalWriter(
                mockClientFactory(pravegaWriter), eventRouter, writerMode);
        Assert.assertSame(eventRouter, writer.getEventRouter());
        Assert.assertEquals(writerMode, writer.getPravegaWriterMode());
    }

    @Test
    public void testFlinkSerializer() {
        IntegerSerializationSchema schema = new IntegerSerializationSchema();
        PravegaWriter.FlinkSerializer<Integer> serializer = new PravegaWriter.FlinkSerializer<>(schema);
        Integer val = 42;
        Assert.assertEquals(ByteBuffer.wrap(schema.serialize(val)), serializer.serialize(val));
        try {
            serializer.deserialize(ByteBuffer.wrap(schema.serialize(val)));
            Assert.fail("expected an exception");
        } catch (IllegalStateException e) {
            // expected
        }
    }

    @Test
    @Ignore  // TODO: fix this test!
    public void testTransactionalWriterNormalCase() throws Exception {
        try (WriterTestContext context = new WriterTestContext()) {
            Transaction<Integer> trans = context.prepareTransaction();
            try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.writer)) {
                testHarness.open();
                StreamRecord<Integer> e1 = new StreamRecord<>(1, 1L);
                testHarness.processElement(e1);
                verify(trans).writeEvent(ROUTING_KEY, e1.getValue());

                // verify that the transaction is flushed and tracked as pending
                testHarness.snapshot(1L, 1L);
                verify(trans).flush();

                // verify commit of transactions up to checkpointId (trans)
                Mockito.when(trans.checkStatus()).thenReturn(Transaction.Status.OPEN);
                testHarness.notifyOfCompletedCheckpoint(1L);
                verify(trans).commit();
            }
        }
    }
    // --------- utilities ---------

    @SuppressWarnings("unchecked")
    private <T> EventStreamWriter<T> mockEventStreamWriter() {
        return mock(EventStreamWriter.class);
    }

    @SuppressWarnings("unchecked")
    private <T> TransactionalEventStreamWriter<T> mockTxnEventStreamWriter() {
        return mock(TransactionalEventStreamWriter.class);
    }

    @SuppressWarnings("unchecked")
    private <T> Transaction<T> mockTransaction() {
        return mock(Transaction.class);
    }

    private <T> EventStreamClientFactory mockClientFactory(EventStreamWriter<T> eventWriter) {
        EventStreamClientFactory clientFactory = mock(EventStreamClientFactory.class);
        when(clientFactory.<T>createEventWriter(anyString(), anyObject(), anyObject())).thenReturn(eventWriter);
        when(clientFactory.<T>createEventWriter(anyString(), anyString(), anyObject(), anyObject())).thenReturn(eventWriter);
        return clientFactory;
    }

    private <T> EventStreamClientFactory mockTxnClientFactory(TransactionalEventStreamWriter<T> eventWriter) {
        EventStreamClientFactory clientFactory = mock(EventStreamClientFactory.class);
        when(clientFactory.<T>createTransactionalEventWriter(anyString(), anyObject(), anyObject())).thenReturn(eventWriter);
        when(clientFactory.<T>createTransactionalEventWriter(anyString(), anyString(), anyObject(), anyObject())).thenReturn(eventWriter);
        return clientFactory;
    }

    private FlinkPravegaInternalWriter<Integer> spyInternalWriter(EventStreamClientFactory clientFactory,
                                                                  PravegaEventRouter<Integer> eventRouter,
                                                                  PravegaWriterMode writerMode) {

        final ExecutorService executorService = spy(new DirectExecutorService());

        FlinkPravegaInternalWriter<Integer> writer = spy(new FlinkPravegaInternalWriter<>(
                MOCK_CLIENT_CONFIG, Stream.of(MOCK_SCOPE_NAME, MOCK_STREAM_NAME), DEFAULT_TXN_LEASE_RENEWAL_PERIOD_MILLIS,
                writerMode, new IntegerSerializationSchema(), eventRouter));

        Mockito.doReturn(executorService).when(writer).createExecutorService();
        Mockito.doReturn(clientFactory).when(writer).createClientFactory(anyString(), anyObject());

        writer.initializeInternalWriter();

        return writer;
    }

    private PravegaWriter<Integer> spyWriter(EventStreamClientFactory clientFactory,
                                             PravegaEventRouter<Integer> eventRouter,
                                             PravegaWriterMode writerMode) {
        final FlinkPravegaInternalWriter<Integer> internalWriter = spyInternalWriter(clientFactory, eventRouter, writerMode);

        PravegaWriter<Integer> writer = spy(new PravegaWriter<>(mock(Sink.InitContext.class),
                false, MOCK_CLIENT_CONFIG, Stream.of(MOCK_SCOPE_NAME, MOCK_STREAM_NAME),
                DEFAULT_TXN_LEASE_RENEWAL_PERIOD_MILLIS, writerMode, new IntegerSerializationSchema(), eventRouter));

        Mockito.doReturn(internalWriter).when(writer).createFlinkPravegaInternalWriter();

        writer.createFlinkPravegaInternalWriter();

        return writer;
    }

    class WriterTestContext implements AutoCloseable {
        final EventStreamWriter<Integer> pravegaWriter;
        final TransactionalEventStreamWriter<Integer> pravegaTxnWriter;
        final PravegaEventRouter<Integer> eventRouter;
        final ExecutorService executorService;
        final FlinkPravegaInternalWriter<Integer> sinkFunction;
        final FlinkPravegaInternalWriter<Integer> txnSinkFunction;
        final PravegaWriter<Integer> writer;

        WriterTestContext() {
            pravegaWriter = mockEventStreamWriter();
            pravegaTxnWriter = mockTxnEventStreamWriter();
            eventRouter = FIXED_EVENT_ROUTER;

            sinkFunction = spyInternalWriter(mockClientFactory(pravegaWriter), eventRouter, PravegaWriterMode.ATLEAST_ONCE);
            txnSinkFunction = spyInternalWriter(mockTxnClientFactory(pravegaTxnWriter), eventRouter, PravegaWriterMode.EXACTLY_ONCE);
            writer = spyWriter(mockTxnClientFactory(pravegaTxnWriter), eventRouter, PravegaWriterMode.EXACTLY_ONCE);

            // inject an instrumented, direct executor
            executorService = spy(new DirectExecutorService());
            Mockito.doReturn(executorService).when(sinkFunction).createExecutorService();
            Mockito.doReturn(executorService).when(txnSinkFunction).createExecutorService();
        }

        @Override
        public void close() throws Exception {
        }

        CompletableFuture<Void> prepareWrite() {
            CompletableFuture<Void> writeFuture = new CompletableFuture<>();
            when(pravegaWriter.writeEvent(anyString(), anyObject())).thenReturn(writeFuture);
            return writeFuture;
        }

        Transaction<Integer> prepareTransaction() {
            Transaction<Integer> trans = mockTransaction();
            UUID txnId = UUID.randomUUID();
            Mockito.doReturn(txnId).when(trans).getTxnId();
            Mockito.doReturn(trans).when(pravegaTxnWriter).beginTxn();
            Mockito.doReturn(trans).when(pravegaTxnWriter).getTxn(txnId);
            return trans;
        }
    }

    private StreamSinkOperatorTestHarness<Integer> createTestHarness(PravegaWriter<Integer> writer) throws Exception {
        return new StreamSinkOperatorTestHarness<>(
                new SinkOperatorFactory<>(mockSink(writer), false, true),
                IntSerializer.INSTANCE);
    }

    @SuppressWarnings("unchecked")
    private <T> PravegaSink<T> mockSink(PravegaWriter<T> writer) throws IOException {
        PravegaSink<T> sink = mock(PravegaSink.class);

        Mockito.doReturn(writer).when(sink).createWriter(anyObject(), anyObject());

        return sink;
    }
}
