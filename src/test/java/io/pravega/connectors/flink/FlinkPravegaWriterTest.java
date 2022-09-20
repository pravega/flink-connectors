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
package io.pravega.connectors.flink;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.common.function.RunnableWithException;
import io.pravega.connectors.flink.serialization.FlinkSerializer;
import io.pravega.connectors.flink.utils.DirectExecutorService;
import io.pravega.connectors.flink.utils.IntegerSerializationSchema;
import io.pravega.connectors.flink.utils.StreamSinkOperatorTestHarness;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.MockSerializationSchema;
import org.apache.flink.util.ExceptionUtils;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

import static io.pravega.connectors.flink.AbstractStreamingWriterBuilder.DEFAULT_TXN_LEASE_RENEWAL_PERIOD_MILLIS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FlinkPravegaWriterTest {

    // region Constants

    private static final ClientConfig MOCK_CLIENT_CONFIG = ClientConfig.builder().build();
    private static final String MOCK_SCOPE_NAME = "scope";
    private static final String MOCK_STREAM_NAME = "stream";
    private static final String ROUTING_KEY = "fixed";

    // endregion

    // region DSL

    /**
     * Tests the constructor.
     */
    @Test
    public void testConstructor() {
        EventStreamWriter<Integer> pravegaWriter = mockEventStreamWriter();
        PravegaEventRouter<Integer> eventRouter = new FixedEventRouter<>();
        PravegaWriterMode writerMode = PravegaWriterMode.ATLEAST_ONCE;
        FlinkPravegaWriter<Integer> sinkFunction = spySinkFunction(mockClientFactory(pravegaWriter), eventRouter, true, writerMode);
        assertThat(sinkFunction.getEventRouter()).isSameAs(eventRouter);
        assertThat(sinkFunction.getPravegaWriterMode()).isEqualTo(writerMode);
        assertThat(sinkFunction.getEnableWatermark()).isTrue();
    }

    // endregion

    // region Lifecycle

    /**
     * Tests the open/close lifecycle methods.
     */
    @Test
    public void testOpenClose() throws Exception {
        EventStreamWriter<Integer> pravegaWriter = mockEventStreamWriter();
        EventStreamClientFactory clientFactory = mockClientFactory(pravegaWriter);
        FlinkPravegaWriter<Integer> sinkFunction = spySinkFunction(clientFactory, null, false, PravegaWriterMode.ATLEAST_ONCE);

        try {
            try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(sinkFunction.ignoreFailuresAfterTransactionTimeout())) {
                testHarness.open();

                // verify that exceptions don't interfere with close
                Mockito.doThrow(new IntentionalRuntimeException()).when(pravegaWriter).close();
                Mockito.doThrow(new IntentionalRuntimeException()).when(clientFactory).close();
            }
            fail(null);
        } catch (IntentionalRuntimeException e) {
            assertThat(e.getSuppressed().length == 1).isTrue();
            assertThat(e.getSuppressed()[0] instanceof IntentionalRuntimeException).isTrue();
        }

        verify(clientFactory).close();
    }

    /**
     * Tests the open method for serializationSchema.
     */
    @Test
    public void testOpenSerializationSchema() throws Exception {
        MockSerializationSchema<Integer> schema = new MockSerializationSchema<>();

        ExecutorService executorService = spy(new DirectExecutorService());

        EventStreamWriter<Integer> pravegaWriter = mockEventStreamWriter();
        EventStreamClientFactory clientFactory = mockClientFactory(pravegaWriter);
        FlinkPravegaWriter<Integer> sinkFunction = spy(new FlinkPravegaWriter<>(
                MOCK_CLIENT_CONFIG, Stream.of(MOCK_SCOPE_NAME, MOCK_STREAM_NAME), schema,
                null, PravegaWriterMode.ATLEAST_ONCE, DEFAULT_TXN_LEASE_RENEWAL_PERIOD_MILLIS,
                false, true));
        Mockito.doReturn(executorService).when(sinkFunction).createExecutorService();
        Mockito.doReturn(clientFactory).when(sinkFunction).createClientFactory(MOCK_SCOPE_NAME, MOCK_CLIENT_CONFIG);

        StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(sinkFunction);

        testHarness.open();
        assertThat(schema.isOpenCalled()).isTrue();
    }

    // endregion

    /**
     * Tests the internal serializer.
     */
    @Test
    public void testFlinkSerializer() {
        IntegerSerializationSchema schema = new IntegerSerializationSchema();
        FlinkSerializer<Integer> serializer = new FlinkSerializer<>(schema);
        Integer val = 42;
        assertThat(serializer.serialize(val)).isEqualTo(ByteBuffer.wrap(schema.serialize(val)));
        try {
            serializer.deserialize(ByteBuffer.wrap(schema.serialize(val)));
            fail("expected an exception");
        } catch (IllegalStateException e) {
            // expected
        }
    }

    // endregion

    // region NonTransactionalWriter

    /**
     * Tests the {@code processElement} method.
     * See also: {@code testNonTransactionalWriterProcessElementAccounting}, {@code testNonTransactionalWriterProcessElementErrorHandling}
     */
    @Test
    public void testNonTransactionalWriterProcessElementWrite() throws Exception {
        try (WriterTestContext context = new WriterTestContext(false)) {
            try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.sinkFunction)) {
                testHarness.open();
                CompletableFuture<Void> e1Future = context.prepareWrite();
                StreamRecord<Integer> e1 = new StreamRecord<>(1, 1L);
                testHarness.processElement(e1);
                verify(context.pravegaWriter).writeEvent(ROUTING_KEY, e1.getValue());
                e1Future.complete(null);
            }
        }
    }

    /**
     * Tests the accounting of pending writes.
     */
    @Test
    public void testNonTransactionalWriterProcessElementAccounting() throws Exception {
        try (WriterTestContext context = new WriterTestContext(false)) {
            try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.sinkFunction)) {
                testHarness.open();

                CompletableFuture<Void> e1Future = context.prepareWrite();
                StreamRecord<Integer> e1 = new StreamRecord<>(1, 1L);
                testHarness.processElement(e1);
                assertThat(context.sinkFunction.pendingWritesCount.get()).isEqualTo(1);

                CompletableFuture<Void> e2Future = context.prepareWrite();
                StreamRecord<Integer> e2 = new StreamRecord<>(2, 2L);
                testHarness.processElement(e2);
                assertThat(context.sinkFunction.pendingWritesCount.get()).isEqualTo(2);

                CompletableFuture<Void> e3Future = context.prepareWrite();
                StreamRecord<Integer> e3 = new StreamRecord<>(3, 3L);
                testHarness.processElement(e3);
                assertThat(context.sinkFunction.pendingWritesCount.get()).isEqualTo(3);

                e1Future.complete(null);
                e2Future.completeExceptionally(new IntentionalRuntimeException());
                e3Future.complete(null);
                assertThat(context.sinkFunction.pendingWritesCount.get()).isEqualTo(0);

                // clear the error for test simplicity
                context.sinkFunction.writeError.set(null);
            }
        }
    }

    /**
     * Tests the handling of write errors.
     */
    @Test
    public void testNonTransactionalWriterProcessElementErrorHandling() throws Exception {
        try (WriterTestContext context = new WriterTestContext(false)) {
            try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.sinkFunction)) {
                testHarness.open();

                CompletableFuture<Void> e1Future = context.prepareWrite();
                StreamRecord<Integer> e1 = new StreamRecord<>(1, 1L);
                testHarness.processElement(e1);
                e1Future.completeExceptionally(new IntentionalRuntimeException());
                assertThat(context.sinkFunction.writeError.get()).isNotNull();

                StreamRecord<Integer> e2 = new StreamRecord<>(2, 2L);
                try {
                    testHarness.processElement(e2);
                    fail("expected an IOException due to a prior write error");
                } catch (IOException e) {
                    // expected
                }

                // clear the error for test simplicity
                context.sinkFunction.writeError.set(null);
            }
        }
    }

    /**
     * Tests the handling of flushes, which occur upon snapshot and close.
     */
    @Test
    public void testNonTransactionalWriterFlush() throws Exception {
        try (WriterTestContext context = new WriterTestContext(false)) {
            try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.sinkFunction)) {
                testHarness.open();

                // invoke a flush, expecting it to block on pending writes
                context.sinkFunction.pendingWritesCount.incrementAndGet();
                Future<Void> flushFuture = runAsync(context.sinkFunction::flushAndVerify);
                Thread.sleep(1000);
                assertThat(flushFuture.isDone()).isFalse();

                // allow the flush to complete
                synchronized (context.sinkFunction) {
                    context.sinkFunction.pendingWritesCount.decrementAndGet();
                    context.sinkFunction.notify();
                }
                flushFuture.get();

                // invoke another flush following a write error, expecting failure
                context.sinkFunction.writeError.set(new IntentionalRuntimeException());
                try {
                    context.sinkFunction.flushAndVerify();
                    fail("expected an IOException due to a prior write error");
                } catch (IOException e) {
                    // expected
                }

                verify(context.pravegaWriter, times(2)).flush();
            }
        }
    }

    /**
     * Tests the {@code close} method.
     */
    @Test
    public void testNonTransactionalWriterClose() throws Exception {
        try (WriterTestContext context = new WriterTestContext(false)) {
            try {
                try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.sinkFunction)) {
                    testHarness.open();

                    // prepare a worst-case situation that exercises the exception handling aspect of close
                    context.sinkFunction.writeError.set(new IntentionalRuntimeException());
                    Mockito.doThrow(new IntentionalRuntimeException()).when(context.pravegaWriter).close();
                    Mockito.doThrow(new IntentionalRuntimeException()).when(context.executorService).shutdown();
                }
                fail("expected an exception");
            } catch (IOException e) {
                assertThat(e.getSuppressed().length).isEqualTo(2);
                assertThat(e.getSuppressed()[0] instanceof IntentionalRuntimeException).isTrue();
                assertThat(e.getSuppressed()[1] instanceof IntentionalRuntimeException).isTrue();
            }
        }
    }

    /**
     * Tests the {@code snapshotState} method.
     */
    @Test
    public void testNonTransactionalWriterSnapshot() throws Exception {
        try (WriterTestContext context = new WriterTestContext(false)) {
            try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.sinkFunction)) {
                testHarness.open();

                // take a snapshot
                testHarness.snapshot(1L, 1L);

                // simulate a write error
                context.sinkFunction.writeError.set(new IntentionalRuntimeException());

                // take another snapshot, expecting it to fail
                try {
                    testHarness.snapshot(2L, 2L);
                    fail("expected an exception due to a prior write error");
                } catch (Exception ex) {
                    assertThat(ex.getCause()).isNotNull();
                    Optional<IOException> exCause = ExceptionUtils.findSerializedThrowable(ex, IOException.class,
                            ClassLoader.getSystemClassLoader());
                    Optional<IntentionalRuntimeException> exRootCause = ExceptionUtils.findSerializedThrowable(ex.getCause(),
                            IntentionalRuntimeException.class, ClassLoader.getSystemClassLoader());
                    assertThat(exCause.isPresent()).isTrue();
                    assertThat(exRootCause.isPresent()).isTrue();
                }

                // clear the error for test simplicity
                context.sinkFunction.writeError.set(null);
            }
        }
    }

    // endregion

    // region TransactionalWriter

    /**
     * Tests the open method of the transactional writer.
     */
    @Test
    public void testTransactionalWriterOpen() throws Exception {
        try (WriterTestContext context = new WriterTestContext(false)) {
            try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.txnSinkFunction)) {
                // open the sink, expecting an initial transaction
                context.prepareTransaction();
                testHarness.open();
                assertThat(context.pravegaTxnWriter).isNotNull();
                verify(context.pravegaTxnWriter).beginTxn();
            }
        }
    }

    /**
     * Tests the close method of the transactional writer.
     */
    @Test
    public void testTransactionalWriterClose() throws Exception {
        try (WriterTestContext context = new WriterTestContext(false)) {
            Transaction<Integer> trans = context.prepareTransaction();
            try {
                try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.txnSinkFunction)) {
                    testHarness.open();

                    // prepare a worst-case situation that exercises the exception handling aspect of close
                    Mockito.doThrow(new IntentionalRuntimeException()).when(trans).abort();
                    Mockito.doThrow(new IntentionalRuntimeException()).when(context.pravegaTxnWriter).close();
                }
                fail("expected an exception");
            } catch (IntentionalRuntimeException e) {
                assertThat(e.getSuppressed().length).isEqualTo(1);
                assertThat(e.getSuppressed()[0] instanceof IntentionalRuntimeException).isTrue();
            }

            // verify that the transaction was aborted and the writer closed
            verify(trans).abort();
            verify(context.pravegaTxnWriter).close();
        }
    }

    /**
     * Tests the {@code processElement} method.
     */
    @Test
    public void testTransactionalWriterNormalCase() throws Exception {
        try (WriterTestContext context = new WriterTestContext(false)) {
            Transaction<Integer> trans = context.prepareTransaction();
            try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.txnSinkFunction)) {
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

    /**
     * Tests the error handling with unknown transaction.
     */
    @Test
    public void testTransactionalWriterCommitWithUnknownId() throws Exception {
        try (WriterTestContext context = new WriterTestContext(false)) {
            Transaction<Integer> trans = context.prepareTransaction();
            try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.txnSinkFunction)) {
                testHarness.open();
                StreamRecord<Integer> e1 = new StreamRecord<>(1, 1L);
                testHarness.processElement(e1);
                testHarness.snapshot(1L, 1L);

                Mockito.when(trans.checkStatus()).thenThrow(new StatusRuntimeException(Status.NOT_FOUND));
                testHarness.notifyOfCompletedCheckpoint(1L);
                // StatusRuntimeException with Unknown transaction is caught
            }
        }
    }

    /**
     * Tests the error handling.
     */
    @Test
    public void testTransactionalWriterCommitFailCase() throws Exception {
        try (WriterTestContext context = new WriterTestContext(false)) {
            Transaction<Integer> trans = context.prepareTransaction();
            try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.txnSinkFunction)) {
                testHarness.open();
                StreamRecord<Integer> e1 = new StreamRecord<>(1, 1L);
                testHarness.processElement(e1);
                testHarness.snapshot(1L, 1L);

                Mockito.when(trans.checkStatus()).thenReturn(Transaction.Status.OPEN);
                Mockito.doThrow(new TxnFailedException()).when(trans).commit();
                testHarness.notifyOfCompletedCheckpoint(1L);
                // TxnFailedException is caught
            }
        }
    }

    /**
     * Tests the wrong transaction status while committing.
     */
    @Test
    public void testTransactionalWriterCommitWrongStatusCase() throws Exception {
        try (WriterTestContext context = new WriterTestContext(false)) {
            Transaction<Integer> trans = context.prepareTransaction();
            try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.txnSinkFunction)) {
                testHarness.open();
                StreamRecord<Integer> e1 = new StreamRecord<>(1, 1L);
                testHarness.processElement(e1);

                testHarness.snapshot(1L, 1L);
                Mockito.when(trans.checkStatus()).thenReturn(Transaction.Status.ABORTED);
                testHarness.notifyOfCompletedCheckpoint(1L);

                verify(trans, never()).commit();
            }
        }
    }

    @Test
    public void testSchemaRegistrySerialization() throws Exception {
        PravegaConfig pravegaConfig = PravegaConfig.fromDefaults();
        try {
            FlinkPravegaWriter.<Integer>builder()
                    .withPravegaConfig(pravegaConfig)
                    .forStream("stream")
                    .withSerializationSchemaFromRegistry("stream", Integer.class)
                    .build();
            fail(null);
        } catch (NullPointerException e) {
            // "missing default scope"
        }

        pravegaConfig.withDefaultScope("scope");
        try {
            FlinkPravegaWriter.<Integer>builder()
                    .withPravegaConfig(pravegaConfig)
                    .forStream("stream")
                    .withSerializationSchemaFromRegistry("stream", Integer.class)
                    .build();
            fail(null);
        } catch (NullPointerException e) {
            // "missing Schema Registry URI"
        }

        pravegaConfig.withSchemaRegistryURI(URI.create("http://localhost:9092"));
        try {
            FlinkPravegaOutputFormat.<Integer>builder()
                    .withPravegaConfig(pravegaConfig)
                    .forStream("stream")
                    .withSerializationSchemaFromRegistry("stream", Integer.class)
                    .build();
        } catch (NotImplementedException e) {
            // "Not support SerializationFormat.Any"
        }
    }

    // endregion

    // region Utilities

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
        when(clientFactory.<T>createTransactionalEventWriter(anyString(), anyString(), anyObject(), anyObject())).thenReturn(eventWriter);
        return clientFactory;
    }

    private FlinkPravegaWriter<Integer> spySinkFunction(EventStreamClientFactory clientFactory,
                                                        PravegaEventRouter<Integer> eventRouter,
                                                        boolean enableWatermark,
                                                        PravegaWriterMode writerMode) {

        final ExecutorService executorService = spy(new DirectExecutorService());

        FlinkPravegaWriter<Integer> sinkFunction = spy(new FlinkPravegaWriter<>(
                MOCK_CLIENT_CONFIG, Stream.of(MOCK_SCOPE_NAME, MOCK_STREAM_NAME), new IntegerSerializationSchema(),
                eventRouter, writerMode, DEFAULT_TXN_LEASE_RENEWAL_PERIOD_MILLIS, enableWatermark, true));

        Mockito.doReturn(executorService).when(sinkFunction).createExecutorService();
        Mockito.doReturn(clientFactory).when(sinkFunction).createClientFactory(MOCK_SCOPE_NAME, MOCK_CLIENT_CONFIG);

        return sinkFunction;
    }

    /**
     * A test context suitable for testing implementations of {@link FlinkPravegaWriter}.
     */
    class WriterTestContext implements AutoCloseable {
        final EventStreamWriter<Integer> pravegaWriter;
        final TransactionalEventStreamWriter<Integer> pravegaTxnWriter;
        final PravegaEventRouter<Integer> eventRouter;
        final ExecutorService executorService;
        final FlinkPravegaWriter<Integer> sinkFunction;
        final FlinkPravegaWriter<Integer> txnSinkFunction;

        WriterTestContext(boolean enableWatermark) {
            pravegaWriter = mockEventStreamWriter();
            pravegaTxnWriter = mockTxnEventStreamWriter();
            eventRouter = new FixedEventRouter<>();

            sinkFunction = spySinkFunction(mockClientFactory(pravegaWriter), eventRouter, enableWatermark, PravegaWriterMode.ATLEAST_ONCE);
            txnSinkFunction = spySinkFunction(mockTxnClientFactory(pravegaTxnWriter), eventRouter, enableWatermark, PravegaWriterMode.EXACTLY_ONCE);

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


    private StreamSinkOperatorTestHarness<Integer> createTestHarness(FlinkPravegaWriter<Integer> sinkFunction) throws Exception {
        return new StreamSinkOperatorTestHarness<>(sinkFunction, IntSerializer.INSTANCE);
    }

    private static Future<Void> runAsync(RunnableWithException runnable) {
        Callable<Void> callable = () -> {
            runnable.run();
            return null;
        };
        return ForkJoinPool.commonPool().submit(callable);
    }

    private static class FixedEventRouter<T> implements PravegaEventRouter<T> {
        @Override
        public String getRoutingKey(T event) {
            return ROUTING_KEY;
        }
    }

    private static class IntentionalRuntimeException extends RuntimeException {

    }

    // endregion
}
