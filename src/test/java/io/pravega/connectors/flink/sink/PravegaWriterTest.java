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
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.common.function.RunnableWithException;
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.connectors.flink.PravegaWriterMode;
import io.pravega.connectors.flink.utils.DirectExecutorService;
import io.pravega.connectors.flink.utils.IntegerSerializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.streaming.runtime.operators.sink.SinkOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

import static io.pravega.connectors.flink.AbstractStreamingWriterBuilder.DEFAULT_TXN_LEASE_RENEWAL_PERIOD_MILLIS;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * These tests cover not only the {@code PravegaWriter} but also {@code FlinkPravegaInternalWriter}
 * and {@code PravegaCommitter} by creating a {@code OneInputStreamOperatorTestHarness} which
 * mimic the process pipeline of the {@code sinkOperator}.
 */
public class PravegaWriterTest {
    private static final ClientConfig MOCK_CLIENT_CONFIG = ClientConfig.builder().build();
    private static final String MOCK_SCOPE_NAME = "scope";
    private static final String MOCK_STREAM_NAME = "stream";
    private static final String ROUTING_KEY = "fixed";
    private static final PravegaEventRouter<Integer> FIXED_EVENT_ROUTER = event -> ROUTING_KEY;

    /**
     * Tests the constructor.
     */
    @Test
    public void testConstructor() {
        final PravegaWriterMode writerMode = PravegaWriterMode.ATLEAST_ONCE;
        final FlinkPravegaInternalWriter<Integer> writer = new TestableFlinkPravegaInternalWriter<>(
                MOCK_CLIENT_CONFIG, Stream.of(MOCK_SCOPE_NAME, MOCK_STREAM_NAME),
                DEFAULT_TXN_LEASE_RENEWAL_PERIOD_MILLIS, writerMode,
                new IntegerSerializationSchema(), FIXED_EVENT_ROUTER);
        Assert.assertSame(FIXED_EVENT_ROUTER, writer.eventRouter);
        Assert.assertEquals(writerMode, writer.writerMode);
    }

    /**
     * Tests the internal serializer.
     */
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

    // region NonTransactionalWriter

    /**
     * Tests the {@code processElement} method.
     * See also: {@code testNonTransactionalWriterProcessElementAccounting}, {@code testNonTransactionalWriterProcessElementErrorHandling}
     */
    @Test
    public void testNonTransactionalWriterWriting() throws Exception {
        final TestablePravegaWriter<Integer> writer = new TestablePravegaWriter<>(
                PravegaWriterMode.ATLEAST_ONCE, new IntegerSerializationSchema());
        final EventStreamWriter<Integer> eventStreamWriter = writer.currentWriter.writer;

        try (OneInputStreamOperatorTestHarness<Integer, byte[]> testHarness =
                     createTestHarness(writer)) {
            testHarness.open();
            assert eventStreamWriter != null;

            CompletableFuture<Void> e1Future = new CompletableFuture<>();
            when(eventStreamWriter.writeEvent(anyString(), anyObject())).thenReturn(e1Future);
            StreamRecord<Integer> e1 = new StreamRecord<>(1, 1L);
            testHarness.processElement(e1);
            verify(eventStreamWriter).writeEvent(ROUTING_KEY, e1.getValue());
            e1Future.complete(null);
        }
    }

    /**
     * Tests the accounting of pending writes.
     */
    @Test
    public void testNonTransactionalWriterProcessElementAccounting() throws Exception {
        final TestablePravegaWriter<Integer> writer = new TestablePravegaWriter<>(
                PravegaWriterMode.ATLEAST_ONCE, new IntegerSerializationSchema());
        final FlinkPravegaInternalWriter<Integer> internalWriter = writer.currentWriter;
        final EventStreamWriter<Integer> eventStreamWriter = internalWriter.writer;

        try (OneInputStreamOperatorTestHarness<Integer, byte[]> testHarness =
                     createTestHarness(writer)) {
            testHarness.open();
            assert eventStreamWriter != null;

            CompletableFuture<Void> e1Future = new CompletableFuture<>();
            when(eventStreamWriter.writeEvent(anyString(), anyObject())).thenReturn(e1Future);
            StreamRecord<Integer> e1 = new StreamRecord<>(1, 1L);
            testHarness.processElement(e1);
            Assert.assertEquals(1, internalWriter.pendingWritesCount.get());

            CompletableFuture<Void> e2Future = new CompletableFuture<>();
            when(eventStreamWriter.writeEvent(anyString(), anyObject())).thenReturn(e2Future);
            StreamRecord<Integer> e2 = new StreamRecord<>(2, 2L);
            testHarness.processElement(e2);
            Assert.assertEquals(2, internalWriter.pendingWritesCount.get());

            CompletableFuture<Void> e3Future = new CompletableFuture<>();
            when(eventStreamWriter.writeEvent(anyString(), anyObject())).thenReturn(e3Future);
            StreamRecord<Integer> e3 = new StreamRecord<>(3, 3L);
            testHarness.processElement(e3);
            Assert.assertEquals(3, internalWriter.pendingWritesCount.get());

            e1Future.complete(null);
            e2Future.completeExceptionally(new IntentionalRuntimeException());
            e3Future.complete(null);
            Assert.assertEquals(0, internalWriter.pendingWritesCount.get());

            // clear the error for test simplicity
            internalWriter.writeError.set(null);
        }
    }

    /**
     * Tests the handling of write errors.
     */
    @Test
    public void testNonTransactionalWriterProcessElementErrorHandling() throws Exception {
        final TestablePravegaWriter<Integer> writer = new TestablePravegaWriter<>(
                PravegaWriterMode.ATLEAST_ONCE, new IntegerSerializationSchema());
        final FlinkPravegaInternalWriter<Integer> internalWriter = writer.currentWriter;
        final EventStreamWriter<Integer> eventStreamWriter = internalWriter.writer;

        try (OneInputStreamOperatorTestHarness<Integer, byte[]> testHarness =
                     createTestHarness(writer)) {
            testHarness.open();
            assert eventStreamWriter != null;

            CompletableFuture<Void> e1Future = new CompletableFuture<>();
            when(eventStreamWriter.writeEvent(anyString(), anyObject())).thenReturn(e1Future);
            StreamRecord<Integer> e1 = new StreamRecord<>(1, 1L);
            testHarness.processElement(e1);
            e1Future.completeExceptionally(new IntentionalRuntimeException());
            Assert.assertNotNull(internalWriter.writeError.get());

            StreamRecord<Integer> e2 = new StreamRecord<>(2, 2L);
            try {
                testHarness.processElement(e2);
                Assert.fail("expected an IOException due to a prior write error");
            } catch (IOException e) {
                // expected IOException due to a prior write error
            }

            // clear the error for test simplicity
            internalWriter.writeError.set(null);
        }
    }

    /**
     * Tests the handling of flushes, which occur upon snapshot and close.
     */
    @Test
    public void testNonTransactionalWriterFlush() throws Exception {
        final TestablePravegaWriter<Integer> writer = new TestablePravegaWriter<>(
                PravegaWriterMode.ATLEAST_ONCE, new IntegerSerializationSchema());
        final FlinkPravegaInternalWriter<Integer> internalWriter = writer.currentWriter;
        final EventStreamWriter<Integer> eventStreamWriter = internalWriter.writer;

        try (OneInputStreamOperatorTestHarness<Integer, byte[]> testHarness =
                     createTestHarness(writer)) {
            testHarness.open();
            assert eventStreamWriter != null;

            // invoke a flush, expecting it to block on pending writes
            internalWriter.pendingWritesCount.incrementAndGet();
            Future<Void> flushFuture = runAsync(internalWriter::flushAndVerify);
            Thread.sleep(1000);
            Assert.assertFalse(flushFuture.isDone());

            // allow the flush to complete
            synchronized (writer.currentWriter) {
                internalWriter.pendingWritesCount.decrementAndGet();
                internalWriter.notify();
            }
            flushFuture.get();

            // invoke another flush following a write error, expecting failure
            internalWriter.writeError.set(new IntentionalRuntimeException());
            try {
                internalWriter.flushAndVerify();
                Assert.fail("expected an IOException due to a prior write error");
            } catch (IOException e) {
                // expected IOException due to a prior write error
            }

            verify(eventStreamWriter, times(2)).flush();
        }
    }

    /**
     * Tests the {@code close} method.
     */
    @Test
    public void testNonTransactionalWriterClose() throws Exception {
        final TestablePravegaWriter<Integer> writer = new TestablePravegaWriter<>(
                PravegaWriterMode.ATLEAST_ONCE, new IntegerSerializationSchema());
        final FlinkPravegaInternalWriter<Integer> internalWriter = writer.currentWriter;
        final EventStreamWriter<Integer> eventStreamWriter = internalWriter.writer;

        try {
            try (OneInputStreamOperatorTestHarness<Integer, byte[]> testHarness =
                         createTestHarness(writer)) {
                testHarness.open();
                assert eventStreamWriter != null;

                // prepare a worst-case situation that exercises the exception handling aspect of close
                internalWriter.writeError.set(new IntentionalRuntimeException());
                Mockito.doThrow(new IntentionalRuntimeException()).when(eventStreamWriter).close();
                Mockito.doThrow(new IntentionalRuntimeException())
                        .when(internalWriter.executorService)
                        .shutdown();
            }
        } catch (IOException e) {
            Assert.assertEquals(2, e.getSuppressed().length);
            Assert.assertTrue(e.getSuppressed()[0] instanceof IntentionalRuntimeException);
            Assert.assertTrue(e.getSuppressed()[1] instanceof IntentionalRuntimeException);
        }
    }

    // endregion

    // region TransactionalWriter

    // endregion

    private static class TestablePravegaWriter<T> extends PravegaWriter<T> {
        public TestablePravegaWriter(Sink.InitContext context,
                                     boolean enableMetrics,
                                     ClientConfig clientConfig,
                                     Stream stream,
                                     long txnLeaseRenewalPeriod,
                                     PravegaWriterMode writerMode,
                                     SerializationSchema<T> serializationSchema,
                                     @Nullable PravegaEventRouter<T> eventRouter) {
            super(context, enableMetrics, clientConfig, stream, txnLeaseRenewalPeriod,
                    writerMode, serializationSchema, eventRouter);
        }

        public TestablePravegaWriter(PravegaWriterMode writerMode, SerializationSchema<T> serializationSchema) {
            this(mock(Sink.InitContext.class), false, MOCK_CLIENT_CONFIG,
                    Stream.of(MOCK_SCOPE_NAME, MOCK_STREAM_NAME),
                    DEFAULT_TXN_LEASE_RENEWAL_PERIOD_MILLIS, writerMode,
                    serializationSchema, event -> ROUTING_KEY);
        }

        @Override
        protected FlinkPravegaInternalWriter<T> createFlinkPravegaInternalWriter() {
            return new TestableFlinkPravegaInternalWriter<>(clientConfig, stream,
                    txnLeaseRenewalPeriod, writerMode, serializationSchema, eventRouter);
        }
    }

    private static class TestableFlinkPravegaInternalWriter<T> extends FlinkPravegaInternalWriter<T> {
        // use these mocks instead of the original ones
        EventStreamWriter<Integer> pravegaWriter;
        TransactionalEventStreamWriter<Integer> pravegaTxnWriter;

        Transaction<T> trans = mockTransaction();
        UUID txnId = UUID.randomUUID();

        public TestableFlinkPravegaInternalWriter(ClientConfig clientConfig,
                                                  Stream stream,
                                                  long txnLeaseRenewalPeriod,
                                                  PravegaWriterMode writerMode,
                                                  SerializationSchema<T> serializationSchema,
                                                  PravegaEventRouter<T> eventRouter) {
            super();
            this.clientConfig = clientConfig;
            this.stream = stream;
            this.txnLeaseRenewalPeriod = txnLeaseRenewalPeriod;
            this.writerMode = writerMode;
            this.serializationSchema = serializationSchema;
            this.eventRouter = eventRouter;

            initializeInternalWriter();

            // they are for the trans inside the PravegaWriter
            Mockito.doReturn(txnId).when(trans).getTxnId();
            Mockito.doReturn(trans).when(pravegaTxnWriter).beginTxn();
            Mockito.doReturn(Transaction.Status.OPEN).when(trans).checkStatus();

            // this is for the trans inside PravegaCommitter
            Mockito.doAnswer(ans -> {
                // update the exposed trans with the latest resumed txnId
                UUID txnId = ans.getArgumentAt(0, UUID.class);
                Mockito.doReturn(txnId).when(trans).getTxnId();
                // mock it ready for the commit
                Mockito.doReturn(Transaction.Status.OPEN).when(trans).checkStatus();
                return trans;
            }).when(pravegaTxnWriter).getTxn(any(UUID.class));
        }

        @Override
        protected void initializeInternalWriter() {
            // use our custom mocked client factory
            clientFactory = createMockedClientFactory();
            createInternalWriter(clientFactory);
        }

        @Override
        protected void createInternalWriter(EventStreamClientFactory clientFactory) {
            Serializer<T> eventSerializer = new PravegaWriter.FlinkSerializer<>(serializationSchema);
            EventWriterConfig writerConfig = EventWriterConfig.builder()
                    .transactionTimeoutTime(txnLeaseRenewalPeriod)
                    .build();
            if (writerMode == PravegaWriterMode.EXACTLY_ONCE) {
                transactionalWriter = clientFactory.createTransactionalEventWriter(stream.getStreamName(), eventSerializer, writerConfig);
            } else {
                executorService = spy(new DirectExecutorService());
                writer = clientFactory.createEventWriter(stream.getStreamName(), eventSerializer, writerConfig);
            }
        }

        private EventStreamClientFactory createMockedClientFactory() {
            pravegaWriter = mockEventStreamWriter();
            pravegaTxnWriter = mockTxnEventStreamWriter();
            return mockClientFactory(pravegaWriter, pravegaTxnWriter);
        }
    }

    // region mock items

    @SuppressWarnings("unchecked")
    private static <T> EventStreamWriter<T> mockEventStreamWriter() {
        return mock(EventStreamWriter.class);
    }

    @SuppressWarnings("unchecked")
    private static <T> TransactionalEventStreamWriter<T> mockTxnEventStreamWriter() {
        return mock(TransactionalEventStreamWriter.class);
    }

    @SuppressWarnings("unchecked")
    private static <T> Transaction<T> mockTransaction() {
        return mock(Transaction.class);
    }

    private static <T> EventStreamClientFactory mockEventClientFactory(EventStreamWriter<T> eventWriter) {
        EventStreamClientFactory clientFactory = mock(EventStreamClientFactory.class);
        when(clientFactory.<T>createEventWriter(anyString(), anyObject(), anyObject())).thenReturn(eventWriter);
        when(clientFactory.<T>createEventWriter(anyString(), anyString(), anyObject(), anyObject())).thenReturn(eventWriter);
        return clientFactory;
    }

    private static <T> EventStreamClientFactory mockTransactionClientFactory(TransactionalEventStreamWriter<T> txnEventWriter) {
        EventStreamClientFactory clientFactory = mock(EventStreamClientFactory.class);
        when(clientFactory.<T>createTransactionalEventWriter(anyString(), anyObject(), anyObject())).thenReturn(txnEventWriter);
        when(clientFactory.<T>createTransactionalEventWriter(anyString(), anyString(), anyObject(), anyObject())).thenReturn(txnEventWriter);
        return clientFactory;
    }

    private static <T> EventStreamClientFactory mockClientFactory(EventStreamWriter<T> eventWriter, TransactionalEventStreamWriter<T> txnEventWriter) {
        EventStreamClientFactory clientFactory = mock(EventStreamClientFactory.class);
        when(clientFactory.<T>createEventWriter(anyString(), anyObject(), anyObject())).thenReturn(eventWriter);
        when(clientFactory.<T>createEventWriter(anyString(), anyString(), anyObject(), anyObject())).thenReturn(eventWriter);
        when(clientFactory.<T>createTransactionalEventWriter(anyString(), anyObject(), anyObject())).thenReturn(txnEventWriter);
        when(clientFactory.<T>createTransactionalEventWriter(anyString(), anyString(), anyObject(), anyObject())).thenReturn(txnEventWriter);
        return clientFactory;
    }

    private PravegaSink<Integer> mockSink(PravegaWriterMode writerMode,
                                          SinkWriter<Integer, PravegaTransactionState, Void> writer,
                                          @Nullable PravegaCommitter<Integer> committer) throws IOException {
        final PravegaSink<Integer> sink = spy(new PravegaSink<>(false, MOCK_CLIENT_CONFIG,
                Stream.of(MOCK_SCOPE_NAME, MOCK_STREAM_NAME), DEFAULT_TXN_LEASE_RENEWAL_PERIOD_MILLIS,
                writerMode, new IntegerSerializationSchema(), FIXED_EVENT_ROUTER));

        Mockito.doReturn(writer).when(sink).createWriter(anyObject(), anyObject());
        Mockito.doReturn(committer != null ? Optional.of(committer) : Optional.empty()).when(sink).createCommitter();

        return sink;
    }

    // endregion

    // region utilities

    /**
     * A test harness suitable for ATLEAST_ONCE tests.
     *
     * @param writer An internal writer that contains {@link EventStreamWriter}.
     * @return A test harness.
     */
    private OneInputStreamOperatorTestHarness<Integer, byte[]> createTestHarness(PravegaWriter<Integer> writer) throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new SinkOperatorFactory<>(
                        mockSink(PravegaWriterMode.ATLEAST_ONCE, writer, null), false, true),
                IntSerializer.INSTANCE);
    }

    private static Future<Void> runAsync(RunnableWithException runnable) {
        Callable<Void> callable = () -> {
            runnable.run();
            return null;
        };
        return ForkJoinPool.commonPool().submit(callable);
    }

    private static class IntentionalRuntimeException extends RuntimeException {
    }

    // endregion
}
