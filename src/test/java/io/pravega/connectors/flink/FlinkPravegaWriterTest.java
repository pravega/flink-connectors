/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink;

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.Transaction;
import io.pravega.common.function.RunnableWithException;
import io.pravega.connectors.flink.utils.IntegerSerializationSchema;
import io.pravega.connectors.flink.utils.StreamSinkOperatorTestHarness;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.util.DirectExecutorService;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.function.Function;

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
        FlinkPravegaWriter<Integer> sinkFunction = spySinkFunction(mockClientFactory(pravegaWriter), eventRouter, writerMode);
        Assert.assertSame(eventRouter, sinkFunction.getEventRouter());
        Assert.assertEquals(writerMode, sinkFunction.getPravegaWriterMode());
    }

    // endregion

    // region Lifecycle

    /**
     * Tests the open/close lifecycle methods.
     */
    @Test
    public void testOpenClose() throws Exception {
        ClientFactory clientFactory = mockClientFactory(null);
        FlinkPravegaWriter<Integer> sinkFunction = spySinkFunction(clientFactory, new FixedEventRouter<>(), PravegaWriterMode.ATLEAST_ONCE);
        FlinkPravegaWriter.AbstractInternalWriter internalWriter = mock(FlinkPravegaWriter.AbstractInternalWriter.class);
        Mockito.doReturn(internalWriter).when(sinkFunction).createInternalWriter();

        try {
            try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(sinkFunction)) {
                testHarness.open();
                verify(internalWriter).open();

                // verify that exceptions don't interfere with close
                Mockito.doThrow(new IntentionalRuntimeException()).when(internalWriter).close();
                Mockito.doThrow(new IntentionalRuntimeException()).when(clientFactory).close();
            }
            Assert.fail();
        } catch (IntentionalRuntimeException e) {
            Assert.assertTrue(e.getSuppressed().length == 1);
            Assert.assertTrue(e.getSuppressed()[0] instanceof IntentionalRuntimeException);
        }

        verify(internalWriter).close();
        verify(clientFactory).close();
    }

    // endregion

    // region AbstractInternalWriter

    /**
     * A test context suitable for testing implementations of {@link FlinkPravegaWriter.AbstractInternalWriter}.
     */
    class AbstractInternalWriterTestContext implements AutoCloseable {
        final EventStreamWriter<Integer> pravegaWriter;
        final PravegaEventRouter<Integer> eventRouter;
        final ExecutorService executorService;
        final FlinkPravegaWriter<Integer> sinkFunction;

        AbstractInternalWriterTestContext(PravegaWriterMode writerMode) {
            pravegaWriter = mockEventStreamWriter();
            eventRouter = new FixedEventRouter<>();

            sinkFunction = spySinkFunction(mockClientFactory(pravegaWriter), eventRouter, writerMode);

            // inject an instrumented, direct executor
            executorService = spy(new DirectExecutorService());
            Mockito.doReturn(executorService).when(sinkFunction).createExecutorService();

            // instrument the internal writer
            Mockito.doAnswer(FlinkPravegaWriterTest.this::spyInternalWriter).when(sinkFunction).createInternalWriter();
        }

        @Override
        public void close() throws Exception {
        }
    }

    /**
     * Tests the internal serializer.
     */
    @Test
    public void testFlinkSerializer() {
        IntegerSerializationSchema schema = new IntegerSerializationSchema();
        FlinkPravegaWriter.FlinkSerializer<Integer> serializer = new FlinkPravegaWriter.FlinkSerializer<>(schema);
        Integer val = 42;
        Assert.assertEquals(ByteBuffer.wrap(schema.serialize(val)), serializer.serialize(val));
        try {
            serializer.deserialize(ByteBuffer.wrap(schema.serialize(val)));
            Assert.fail("expected an exception");
        } catch (IllegalStateException e) {
            // expected
        }
    }

    // endregion

    // region NonTransactionalWriter

    /**
     * A test context suitable for testing the {@link FlinkPravegaWriter.NonTransactionalWriter}.
     */
    class NonTransactionalWriterTestContext extends AbstractInternalWriterTestContext {
        NonTransactionalWriterTestContext(PravegaWriterMode writerMode) {
            super(writerMode);
        }

        CompletableFuture<Void> prepareWrite() {
            CompletableFuture<Void> writeFuture = new CompletableFuture<>();
            when(pravegaWriter.writeEvent(anyString(), anyObject())).thenReturn(writeFuture);
            return writeFuture;
        }
    }

    /**
     * Tests the {@code processElement} method.
     * See also: {@code testNonTransactionalWriterProcessElementAccounting}, {@code testNonTransactionalWriterProcessElementErrorHandling}
     */
    @Test
    public void testNonTransactionalWriterProcessElementWrite() throws Exception {
        try (NonTransactionalWriterTestContext context = new NonTransactionalWriterTestContext(PravegaWriterMode.ATLEAST_ONCE)) {
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
        try (NonTransactionalWriterTestContext context = new NonTransactionalWriterTestContext(PravegaWriterMode.ATLEAST_ONCE)) {
            try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.sinkFunction)) {
                testHarness.open();
                FlinkPravegaWriter.NonTransactionalWriter internalWriter = (FlinkPravegaWriter.NonTransactionalWriter) context.sinkFunction.writer;

                CompletableFuture<Void> e1Future = context.prepareWrite();
                StreamRecord<Integer> e1 = new StreamRecord<>(1, 1L);
                testHarness.processElement(e1);
                Assert.assertEquals(1, internalWriter.pendingWritesCount.get());

                CompletableFuture<Void> e2Future = context.prepareWrite();
                StreamRecord<Integer> e2 = new StreamRecord<>(2, 2L);
                testHarness.processElement(e2);
                Assert.assertEquals(2, internalWriter.pendingWritesCount.get());

                CompletableFuture<Void> e3Future = context.prepareWrite();
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
    }

    /**
     * Tests the handling of write errors.
     */
    @Test
    public void testNonTransactionalWriterProcessElementErrorHandling() throws Exception {
        try (NonTransactionalWriterTestContext context = new NonTransactionalWriterTestContext(PravegaWriterMode.ATLEAST_ONCE)) {
            try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.sinkFunction)) {
                testHarness.open();
                FlinkPravegaWriter.NonTransactionalWriter internalWriter = (FlinkPravegaWriter.NonTransactionalWriter) context.sinkFunction.writer;

                CompletableFuture<Void> e1Future = context.prepareWrite();
                StreamRecord<Integer> e1 = new StreamRecord<>(1, 1L);
                testHarness.processElement(e1);
                e1Future.completeExceptionally(new IntentionalRuntimeException());
                Assert.assertNotNull(internalWriter.writeError.get());

                StreamRecord<Integer> e2 = new StreamRecord<>(2, 2L);
                try {
                    testHarness.processElement(e2);
                    Assert.fail("expected an IOException due to a prior write error");
                } catch (IOException e) {
                    // expected
                }

                // clear the error for test simplicity
                internalWriter.writeError.set(null);
            }
        }
    }

    /**
     * Tests the handling of flushes, which occur upon snapshot and close.
     */
    @Test
    public void testNonTransactionalWriterFlush() throws Exception {
        try (NonTransactionalWriterTestContext context = new NonTransactionalWriterTestContext(PravegaWriterMode.ATLEAST_ONCE)) {
            try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.sinkFunction)) {
                testHarness.open();
                FlinkPravegaWriter.NonTransactionalWriter internalWriter = (FlinkPravegaWriter.NonTransactionalWriter) context.sinkFunction.writer;

                // invoke a flush, expecting it to block on pending writes
                internalWriter.pendingWritesCount.incrementAndGet();
                Future<Void> flushFuture = runAsync(internalWriter::flushAndVerify);
                Thread.sleep(1000);
                Assert.assertFalse(flushFuture.isDone());

                // allow the flush to complete
                synchronized (internalWriter) {
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
        try (NonTransactionalWriterTestContext context = new NonTransactionalWriterTestContext(PravegaWriterMode.ATLEAST_ONCE)) {
            try {
                try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.sinkFunction)) {
                    testHarness.open();
                    FlinkPravegaWriter.NonTransactionalWriter internalWriter = (FlinkPravegaWriter.NonTransactionalWriter) context.sinkFunction.writer;

                    // prepare a worst-case situation that exercises the exception handling aspect of close
                    internalWriter.writeError.set(new IntentionalRuntimeException());
                    Mockito.doThrow(new IntentionalRuntimeException()).when(context.pravegaWriter).close();
                    Mockito.doThrow(new IntentionalRuntimeException()).when(context.executorService).shutdown();
                }
                Assert.fail("expected an exception");
            } catch (IOException e) {
                Assert.assertEquals(2, e.getSuppressed().length);
                Assert.assertTrue(e.getSuppressed()[0] instanceof IntentionalRuntimeException);
                Assert.assertTrue(e.getSuppressed()[1] instanceof IntentionalRuntimeException);
            }
        }
    }

    /**
     * Tests the {@code snapshotState} method.
     */
    @Test
    public void testNonTransactionalWriterSnapshot() throws Exception {
        try (NonTransactionalWriterTestContext context = new NonTransactionalWriterTestContext(PravegaWriterMode.ATLEAST_ONCE)) {
            try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.sinkFunction)) {
                testHarness.open();
                FlinkPravegaWriter.NonTransactionalWriter internalWriter = (FlinkPravegaWriter.NonTransactionalWriter) context.sinkFunction.writer;

                // take a snapshot
                testHarness.snapshot(1L, 1L);

                // simulate a write error
                internalWriter.writeError.set(new IntentionalRuntimeException());

                // take another snapshot, expecting it to fail
                try {
                    testHarness.snapshot(2L, 2L);
                    Assert.fail("expected an exception due to a prior write error");
                } catch (Exception ex) {
                    Assert.assertNotNull(ex.getCause());
                    Assert.assertTrue(ex.getCause() instanceof IOException);
                    Assert.assertTrue(ex.getCause().getCause() instanceof IntentionalRuntimeException);
                }

                // clear the error for test simplicity
                internalWriter.writeError.set(null);
            }
        }
    }

    // endregion

    // region TransactionalWriter

    /**
     * A test context suitable for testing the {@link FlinkPravegaWriter.TransactionalWriter}.
     */
    class TransactionalWriterTestContext extends AbstractInternalWriterTestContext {
        TransactionalWriterTestContext(PravegaWriterMode writerMode) {
            super(writerMode);
        }

        Transaction<Integer> prepareTransaction() {
            Transaction<Integer> trans = mockTransaction();
            UUID txnId = UUID.randomUUID();
            Mockito.doReturn(txnId).when(trans).getTxnId();
            Mockito.doReturn(trans).when(pravegaWriter).beginTxn();
            Mockito.doReturn(trans).when(pravegaWriter).getTxn(txnId);
            return trans;
        }
    }

    /**
     * Tests the open method of the transactional writer.
     */
    @Test
    public void testTransactionalWriterOpen() throws Exception {
        try (TransactionalWriterTestContext context = new TransactionalWriterTestContext(PravegaWriterMode.EXACTLY_ONCE)) {
            try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.sinkFunction)) {
                // open the sink, expecting an initial transaction
                Transaction<Integer> trans = context.prepareTransaction();
                testHarness.open();
                Assert.assertTrue(context.sinkFunction.writer instanceof FlinkPravegaWriter.TransactionalWriter);
                FlinkPravegaWriter.TransactionalWriter internalWriter = (FlinkPravegaWriter.TransactionalWriter) context.sinkFunction.writer;
                verify(context.pravegaWriter).beginTxn();
                Assert.assertSame(trans, internalWriter.currentTxn);
            }
        }
    }

    /**
     * Tests the close method of the transactional writer.
     */
    @Test
    public void testTransactionalWriterClose() throws Exception {
        try (TransactionalWriterTestContext context = new TransactionalWriterTestContext(PravegaWriterMode.EXACTLY_ONCE)) {
            Transaction<Integer> trans = context.prepareTransaction();
            try {
                try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.sinkFunction)) {
                    testHarness.open();

                    // prepare a worst-case situation that exercises the exception handling aspect of close
                    Mockito.doThrow(new IntentionalRuntimeException()).when(trans).abort();
                    Mockito.doThrow(new IntentionalRuntimeException()).when(context.pravegaWriter).close();
                }
                Assert.fail("expected an exception");
            } catch (IntentionalRuntimeException e) {
                Assert.assertEquals(1, e.getSuppressed().length);
                Assert.assertTrue(e.getSuppressed()[0] instanceof IntentionalRuntimeException);
            }

            // verify that the transaction was aborted and the writer closed
            verify(trans).abort();
            verify(context.pravegaWriter).close();
        }
    }

    /**
     * Tests the {@code processElement} method.
     */
    @Test
    public void testTransactionalWriterProcessElementWrite() throws Exception {
        try (TransactionalWriterTestContext context = new TransactionalWriterTestContext(PravegaWriterMode.EXACTLY_ONCE)) {
            Transaction<Integer> trans = context.prepareTransaction();
            try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.sinkFunction)) {
                testHarness.open();
                StreamRecord<Integer> e1 = new StreamRecord<>(1, 1L);
                testHarness.processElement(e1);
                verify(trans).writeEvent(ROUTING_KEY, e1.getValue());
            }
        }
    }

    /**
     * Tests the {@code snapshot} method.
     */
    @Test
    public void testTransactionalWriterSnapshotState() throws Exception {
        try (TransactionalWriterTestContext context = new TransactionalWriterTestContext(PravegaWriterMode.EXACTLY_ONCE)) {
            try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.sinkFunction)) {
                Transaction<Integer> trans1 = context.prepareTransaction();
                testHarness.open();
                FlinkPravegaWriter.TransactionalWriter internalWriter = (FlinkPravegaWriter.TransactionalWriter) context.sinkFunction.writer;
                verify(trans1).getTxnId();

                // verify that the transaction is flushed and tracked as pending, and that a new transaction is begun
                Transaction<Integer> trans2 = context.prepareTransaction();
                testHarness.snapshot(1L, 1L);
                verify(trans1).flush();
                Assert.assertEquals(1, internalWriter.txnsPendingCommit.size());
                FlinkPravegaWriter.TransactionAndCheckpoint<Integer> pending1 = (FlinkPravegaWriter.TransactionAndCheckpoint<Integer>) internalWriter.txnsPendingCommit.peek();
                Assert.assertEquals(trans1.getTxnId(), pending1.transaction().getTxnId());
                Assert.assertEquals(1L, pending1.checkpointId());
                Assert.assertNotNull(pending1.toString());
                Assert.assertSame(trans2, internalWriter.currentTxn);
            }
        }
    }

    /**
     * Tests the {@code notifyCheckpointComplete} method.
     */
    @Test
    public void testTransactionalWriterNotifyCheckpointComplete() throws Exception {
        try (TransactionalWriterTestContext context = new TransactionalWriterTestContext(PravegaWriterMode.EXACTLY_ONCE)) {
            try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.sinkFunction)) {
                Transaction<Integer> trans1 = context.prepareTransaction();
                testHarness.open();
                FlinkPravegaWriter.TransactionalWriter internalWriter = (FlinkPravegaWriter.TransactionalWriter) context.sinkFunction.writer;

                // initialize the state to contain some pending commits
                internalWriter.txnsPendingCommit.add(new FlinkPravegaWriter.TransactionAndCheckpoint<>(trans1, 1L));
                Transaction<Integer> trans2 = context.prepareTransaction();
                internalWriter.txnsPendingCommit.add(new FlinkPravegaWriter.TransactionAndCheckpoint<>(trans2, 2L));
                Transaction<Integer> trans3 = context.prepareTransaction();
                internalWriter.txnsPendingCommit.add(new FlinkPravegaWriter.TransactionAndCheckpoint<>(trans3, 3L));

                // verify no matching transactions
                testHarness.notifyOfCompletedCheckpoint(0L);
                verify(trans1, never()).commit();
                verify(trans2, never()).commit();
                verify(trans3, never()).commit();
                Assert.assertEquals(3, internalWriter.txnsPendingCommit.size());

                // verify commit of transactions up to checkpointId (trans1), others remaining.
                testHarness.notifyOfCompletedCheckpoint(1L);
                verify(trans1).commit();
                verify(trans2, never()).commit();
                verify(trans3, never()).commit();
                Assert.assertEquals(2, internalWriter.txnsPendingCommit.size());

                // verify commit of subsumed transactions (trans2, trans3)
                testHarness.notifyOfCompletedCheckpoint(3L);
                verify(trans1).commit();
                verify(trans2).commit();
                verify(trans3).commit();
                Assert.assertEquals(0, internalWriter.txnsPendingCommit.size());
            }
        }
    }

    /**
     * Tests the {@code restoreState} method.
     * Note that {@code restoreState} is called before {@code open}.
     */
    @Test
    public void testTransactionalWriterRestoreState() throws Exception {
        try (TransactionalWriterTestContext context = new TransactionalWriterTestContext(PravegaWriterMode.EXACTLY_ONCE)) {

            // verify the behavior with an empty transaction list
            try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.sinkFunction)) {
                testHarness.setup();
                context.sinkFunction.restoreState(Collections.emptyList());
            }

            // verify the behavior for transactions with various statuses
            Function<Transaction.Status, Transaction<Integer>> test = status -> {
                try {
                    try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.sinkFunction)) {
                        testHarness.setup();
                        Transaction<Integer> trans = context.prepareTransaction();
                        when(trans.checkStatus()).thenReturn(status);
                        context.sinkFunction.restoreState(Collections.singletonList(new FlinkPravegaWriter.PendingTransaction(trans.getTxnId(), MOCK_SCOPE_NAME, MOCK_STREAM_NAME)));
                        verify(trans).checkStatus();
                        return trans;
                    }
                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            };
            verify(test.apply(Transaction.Status.OPEN), times(1)).commit();
            verify(test.apply(Transaction.Status.COMMITTING), never()).commit();
            verify(test.apply(Transaction.Status.COMMITTED), never()).commit();
            verify(test.apply(Transaction.Status.ABORTING), never()).commit();
            verify(test.apply(Transaction.Status.ABORTED), never()).commit();
        }
    }

    // endregion

    // region Utilities

    @SuppressWarnings("unchecked")
    private <T> EventStreamWriter<T> mockEventStreamWriter() {
        return mock(EventStreamWriter.class);
    }

    @SuppressWarnings("unchecked")
    private <T> Transaction<T> mockTransaction() {
        return mock(Transaction.class);
    }

    private <T> ClientFactory mockClientFactory(EventStreamWriter<T> eventWriter) {
        ClientFactory clientFactory = mock(ClientFactory.class);
        when(clientFactory.<T>createEventWriter(anyString(), anyObject(), anyObject())).thenReturn(eventWriter);
        return clientFactory;
    }

    private FlinkPravegaWriter<Integer> spySinkFunction(ClientFactory clientFactory, PravegaEventRouter<Integer> eventRouter, PravegaWriterMode writerMode) {
        FlinkPravegaWriter<Integer> writer = spy(new FlinkPravegaWriter<>(
                MOCK_CLIENT_CONFIG, Stream.of(MOCK_SCOPE_NAME, MOCK_STREAM_NAME), new IntegerSerializationSchema(), eventRouter, writerMode, 30, 30));
        Mockito.doReturn(clientFactory).when(writer).createClientFactory(MOCK_SCOPE_NAME, MOCK_CLIENT_CONFIG);
        return writer;
    }

    private FlinkPravegaWriter.AbstractInternalWriter spyInternalWriter(InvocationOnMock invoke) throws Throwable {
        return spy((FlinkPravegaWriter.AbstractInternalWriter) invoke.callRealMethod());
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
