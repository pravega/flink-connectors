/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.common.function.RunnableWithException;
import io.pravega.connectors.flink.utils.DirectExecutorService;
import io.pravega.connectors.flink.utils.IntegerSerializationSchema;
import io.pravega.connectors.flink.utils.StreamSinkOperatorTestHarness;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.ExceptionUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.function.Function;

import static io.pravega.connectors.flink.AbstractStreamingWriterBuilder.DEFAULT_TXN_LEASE_RENEWAL_PERIOD_MILLIS;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
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

    private static final String MOCK_SCOPE_NAME_1 = "scope1";
    private static final String MOCK_SCOPE_NAME_2 = "scope2";
    private static final String MOCK_STREAM_NAME_1 = "stream1";
    private static final String MOCK_STREAM_NAME_2 = "stream2";
    private static final String MOCK_STREAM_NAME_3 = "stream3";
    private static final String MOCK_STREAM_NAME_4 = "stream4";
    private static final String MOCK_STREAM_NAME_5 = "stream5";

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
        Assert.assertSame(eventRouter, sinkFunction.getEventRouter());
        Assert.assertEquals(writerMode, sinkFunction.getPravegaWriterMode());
        Assert.assertTrue(sinkFunction.getEnableWatermark());
    }

    // endregion

    // region Lifecycle

    /**
     * Tests the open/close lifecycle methods.
     */
    @Test
    public void testOpenClose() throws Exception {
        EventStreamClientFactory clientFactory = mockClientFactory(null);
        FlinkPravegaWriter<Integer> sinkFunction = spySinkFunction(clientFactory, new FixedEventRouter<>(), false, PravegaWriterMode.ATLEAST_ONCE);
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
        final TransactionalEventStreamWriter<Integer> pravegaTxnWriter;
        final PravegaEventRouter<Integer> eventRouter;
        final ExecutorService executorService;
        final FlinkPravegaWriter<Integer> sinkFunction;
        final FlinkPravegaWriter<Integer> txnSinkFunction;

        AbstractInternalWriterTestContext(boolean enableWatermark, PravegaWriterMode writerMode) {
            pravegaWriter = mockEventStreamWriter();
            pravegaTxnWriter = mockTxnEventStreamWriter();
            eventRouter = new FixedEventRouter<>();

            sinkFunction = spySinkFunction(mockClientFactory(pravegaWriter), eventRouter, enableWatermark, writerMode);
            txnSinkFunction = spySinkFunction(mockTxnClientFactory(pravegaTxnWriter), eventRouter, enableWatermark, writerMode);

            // inject an instrumented, direct executor
            executorService = spy(new DirectExecutorService());
            Mockito.doReturn(executorService).when(sinkFunction).createExecutorService();
            Mockito.doReturn(executorService).when(txnSinkFunction).createExecutorService();

            // instrument the internal writer
            Mockito.doAnswer(FlinkPravegaWriterTest.this::spyInternalWriter).when(sinkFunction).createInternalWriter();
            Mockito.doAnswer(FlinkPravegaWriterTest.this::spyInternalWriter).when(txnSinkFunction).createInternalWriter();
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
        NonTransactionalWriterTestContext(boolean enableWatermark, PravegaWriterMode writerMode) {
            super(enableWatermark, writerMode);
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
        try (NonTransactionalWriterTestContext context = new NonTransactionalWriterTestContext(false, PravegaWriterMode.ATLEAST_ONCE)) {
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
        try (NonTransactionalWriterTestContext context = new NonTransactionalWriterTestContext(false, PravegaWriterMode.ATLEAST_ONCE)) {
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
        try (NonTransactionalWriterTestContext context = new NonTransactionalWriterTestContext(false, PravegaWriterMode.ATLEAST_ONCE)) {
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
        try (NonTransactionalWriterTestContext context = new NonTransactionalWriterTestContext(false, PravegaWriterMode.ATLEAST_ONCE)) {
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
        try (NonTransactionalWriterTestContext context = new NonTransactionalWriterTestContext(false, PravegaWriterMode.ATLEAST_ONCE)) {
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
        try (NonTransactionalWriterTestContext context = new NonTransactionalWriterTestContext(false, PravegaWriterMode.ATLEAST_ONCE)) {
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
                    Optional<IOException> exCause = ExceptionUtils.findSerializedThrowable(ex, IOException.class,
                            ClassLoader.getSystemClassLoader());
                    Optional<IntentionalRuntimeException> exRootCause = ExceptionUtils.findSerializedThrowable(ex.getCause(),
                            IntentionalRuntimeException.class, ClassLoader.getSystemClassLoader());
                    Assert.assertTrue(exCause.isPresent());
                    Assert.assertTrue(exRootCause.isPresent());
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
        TransactionalWriterTestContext(boolean enableWatermark, PravegaWriterMode writerMode) {
            super(enableWatermark, writerMode);
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

    /**
     * Tests the open method of the transactional writer.
     */
    @Test
    public void testTransactionalWriterOpen() throws Exception {
        try (TransactionalWriterTestContext context = new TransactionalWriterTestContext(false, PravegaWriterMode.EXACTLY_ONCE)) {
            try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.txnSinkFunction)) {
                // open the sink, expecting an initial transaction
                Transaction<Integer> trans = context.prepareTransaction();
                testHarness.open();
                Assert.assertTrue(context.txnSinkFunction.writer instanceof FlinkPravegaWriter.TransactionalWriter);
                FlinkPravegaWriter.TransactionalWriter internalWriter = (FlinkPravegaWriter.TransactionalWriter) context.txnSinkFunction.writer;
                verify(context.pravegaTxnWriter).beginTxn();
                Assert.assertSame(trans, internalWriter.currentTxn);
            }
        }
    }

    /**
     * Tests the close method of the transactional writer.
     */
    @Test
    public void testTransactionalWriterClose() throws Exception {
        try (TransactionalWriterTestContext context = new TransactionalWriterTestContext(false, PravegaWriterMode.EXACTLY_ONCE)) {
            Transaction<Integer> trans = context.prepareTransaction();
            try {
                try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.txnSinkFunction)) {
                    testHarness.open();

                    // prepare a worst-case situation that exercises the exception handling aspect of close
                    Mockito.doThrow(new IntentionalRuntimeException()).when(trans).abort();
                    Mockito.doThrow(new IntentionalRuntimeException()).when(context.pravegaTxnWriter).close();
                }
                Assert.fail("expected an exception");
            } catch (IntentionalRuntimeException e) {
                Assert.assertEquals(1, e.getSuppressed().length);
                Assert.assertTrue(e.getSuppressed()[0] instanceof IntentionalRuntimeException);
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
    public void testTransactionalWriterProcessElementWrite() throws Exception {
        try (TransactionalWriterTestContext context = new TransactionalWriterTestContext(false, PravegaWriterMode.EXACTLY_ONCE)) {
            Transaction<Integer> trans = context.prepareTransaction();
            try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.txnSinkFunction)) {
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
        try (TransactionalWriterTestContext context = new TransactionalWriterTestContext(false, PravegaWriterMode.EXACTLY_ONCE)) {
            try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.txnSinkFunction)) {
                Transaction<Integer> trans1 = context.prepareTransaction();
                testHarness.open();
                FlinkPravegaWriter.TransactionalWriter internalWriter = (FlinkPravegaWriter.TransactionalWriter) context.txnSinkFunction.writer;
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
        try (TransactionalWriterTestContext context = new TransactionalWriterTestContext(false, PravegaWriterMode.EXACTLY_ONCE)) {
            try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.txnSinkFunction)) {
                Transaction<Integer> trans1 = context.prepareTransaction();
                testHarness.open();
                FlinkPravegaWriter.TransactionalWriter internalWriter = (FlinkPravegaWriter.TransactionalWriter) context.txnSinkFunction.writer;

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
        try (TransactionalWriterTestContext context = new TransactionalWriterTestContext(false, PravegaWriterMode.EXACTLY_ONCE)) {

            // verify the behavior with an empty transaction list
            try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.txnSinkFunction)) {
                testHarness.setup();
                context.txnSinkFunction.restoreState(Collections.emptyList());
            }

            // verify the behavior for transactions with various statuses
            Function<Transaction.Status, Transaction<Integer>> test = status -> {
                try {
                    try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.txnSinkFunction)) {
                        testHarness.setup();
                        Transaction<Integer> trans = context.prepareTransaction();
                        when(trans.checkStatus()).thenReturn(status);
                        context.txnSinkFunction.restoreState(Collections.singletonList(
                                new FlinkPravegaWriter.PendingTransaction(trans.getTxnId(), MOCK_SCOPE_NAME, MOCK_STREAM_NAME, null)));
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


    /**
     * Tests the {@code restoreState} method by simulating with multiple transactions from different streams.
     *
     */
    @Test
    public void testTransactionalWriterRestoreStateForMultipleStreams() throws Exception {
        try (TransactionalWriterTestContext context = new TransactionalWriterTestContext(false, PravegaWriterMode.EXACTLY_ONCE)) {
            List<Transaction<Integer>> pendingTransactions = prepareMockInstancesForRestore(context.txnSinkFunction);

            // verify the behavior by passing multiple transaction instances.
            Map<Transaction.Status, List<Transaction<Integer>>> txnMap = new HashMap<>();
            List<FlinkPravegaWriter.PendingTransaction> pendingTransactionList = new ArrayList<>();
            int count = 5;

            try (StreamSinkOperatorTestHarness<Integer> testHarness = createTestHarness(context.txnSinkFunction)) {
                testHarness.setup();

                for (int i = 0; i < count; i++) {
                    Transaction<Integer> integerTransaction = pendingTransactions.get(i);
                    Transaction.Status status;

                    if (i % 2 == 0) {
                        status = Transaction.Status.OPEN;
                    } else {
                        if (i == 1) {
                            status = Transaction.Status.COMMITTING;
                        } else {
                            status = Transaction.Status.ABORTED;
                        }
                    }

                    switch (i) {
                        case 0:
                            pendingTransactionList.add(
                                    new FlinkPravegaWriter.PendingTransaction(integerTransaction.getTxnId(), MOCK_SCOPE_NAME_1, MOCK_STREAM_NAME_1, null));
                            break;
                        case 1:
                            pendingTransactionList.add(
                                    new FlinkPravegaWriter.PendingTransaction(integerTransaction.getTxnId(), MOCK_SCOPE_NAME_1, MOCK_STREAM_NAME_2, null));
                            break;
                        case 2:
                            pendingTransactionList.add(
                                    new FlinkPravegaWriter.PendingTransaction(integerTransaction.getTxnId(), MOCK_SCOPE_NAME_1, MOCK_STREAM_NAME_3, null));
                            break;
                        case 3:
                            pendingTransactionList.add(
                                    new FlinkPravegaWriter.PendingTransaction(integerTransaction.getTxnId(), MOCK_SCOPE_NAME_2, MOCK_STREAM_NAME_4, null));
                            break;
                        case 4:
                            pendingTransactionList.add(
                                    new FlinkPravegaWriter.PendingTransaction(integerTransaction.getTxnId(), MOCK_SCOPE_NAME_2, MOCK_STREAM_NAME_5, null));
                            break;
                        default:
                            break;
                    }

                    when(integerTransaction.checkStatus()).thenReturn(status);

                    if (txnMap.containsKey(status)) {
                        txnMap.get(status).add(integerTransaction);
                    } else {
                        List<Transaction<Integer>> txnList = new ArrayList<>();
                        txnList.add(integerTransaction);
                        txnMap.put(status, txnList);
                    }
                }
                context.txnSinkFunction.restoreState(pendingTransactionList);

                for (Transaction.Status status: txnMap.keySet()) {
                    if (status == Transaction.Status.OPEN) {
                        for (Transaction<Integer> txn: txnMap.get(status)) {
                            verify(txn, times(1)).commit();
                        }
                    } else if (status == Transaction.Status.COMMITTING || status == Transaction.Status.ABORTED) {
                        for (Transaction<Integer> txn: txnMap.get(status)) {
                            verify(txn, never()).commit();
                        }
                    }
                }
            }
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

    private FlinkPravegaWriter<Integer> spySinkFunction(EventStreamClientFactory clientFactory, PravegaEventRouter<Integer> eventRouter, boolean enableWatermark, PravegaWriterMode writerMode) {
        FlinkPravegaWriter<Integer> writer = spy(new FlinkPravegaWriter<>(
                MOCK_CLIENT_CONFIG, Stream.of(MOCK_SCOPE_NAME, MOCK_STREAM_NAME), new IntegerSerializationSchema(),
                eventRouter, writerMode, DEFAULT_TXN_LEASE_RENEWAL_PERIOD_MILLIS, enableWatermark, true));
        Mockito.doReturn(clientFactory).when(writer).createClientFactory(MOCK_SCOPE_NAME, MOCK_CLIENT_CONFIG);
        return writer;
    }

    private <T> List<Transaction<Integer>> prepareMockInstancesForRestore(FlinkPravegaWriter<Integer> writer) {

        EventStreamClientFactory clientFactory = mock(EventStreamClientFactory.class);
        List<Transaction<Integer>> transactions = new ArrayList<>();

        TransactionalEventStreamWriter<T> txnEventStreamWriter1 = mockTxnEventStreamWriter();
        Mockito.doReturn(clientFactory).when(writer).createClientFactory(MOCK_SCOPE_NAME_1, MOCK_CLIENT_CONFIG);
        Mockito.when(clientFactory.<T>createTransactionalEventWriter(anyString(), eq(MOCK_STREAM_NAME_1), anyObject(), anyObject())).thenReturn(txnEventStreamWriter1);
        Transaction<Integer> trans1 = mockTransaction();
        UUID txnId1 = UUID.randomUUID();
        Mockito.doReturn(txnId1).when(trans1).getTxnId();
        Mockito.doReturn(trans1).when(txnEventStreamWriter1).beginTxn();
        Mockito.doReturn(trans1).when(txnEventStreamWriter1).getTxn(txnId1);
        transactions.add(trans1);

        TransactionalEventStreamWriter<T> txnEventStreamWriter2 = mockTxnEventStreamWriter();
        Mockito.doReturn(clientFactory).when(writer).createClientFactory(MOCK_SCOPE_NAME_1, MOCK_CLIENT_CONFIG);
        Mockito.when(clientFactory.<T>createTransactionalEventWriter(anyString(), eq(MOCK_STREAM_NAME_2), anyObject(), anyObject())).thenReturn(txnEventStreamWriter2);
        Transaction<Integer> trans2 = mockTransaction();
        UUID txnId2 = UUID.randomUUID();
        Mockito.doReturn(txnId2).when(trans2).getTxnId();
        Mockito.doReturn(trans2).when(txnEventStreamWriter2).beginTxn();
        Mockito.doReturn(trans2).when(txnEventStreamWriter2).getTxn(txnId2);
        transactions.add(trans2);

        TransactionalEventStreamWriter<T> txnEventStreamWriter3 = mockTxnEventStreamWriter();
        Mockito.doReturn(clientFactory).when(writer).createClientFactory(MOCK_SCOPE_NAME_1, MOCK_CLIENT_CONFIG);
        Mockito.when(clientFactory.<T>createTransactionalEventWriter(anyString(), eq(MOCK_STREAM_NAME_3), anyObject(), anyObject())).thenReturn(txnEventStreamWriter3);
        Transaction<Integer> trans3 = mockTransaction();
        UUID txnId3 = UUID.randomUUID();
        Mockito.doReturn(txnId3).when(trans3).getTxnId();
        Mockito.doReturn(trans3).when(txnEventStreamWriter3).beginTxn();
        Mockito.doReturn(trans3).when(txnEventStreamWriter3).getTxn(txnId3);
        transactions.add(trans3);

        TransactionalEventStreamWriter<T> txnEventStreamWriter4 = mockTxnEventStreamWriter();
        Mockito.doReturn(clientFactory).when(writer).createClientFactory(MOCK_SCOPE_NAME_2, MOCK_CLIENT_CONFIG);
        Mockito.when(clientFactory.<T>createTransactionalEventWriter(anyString(), eq(MOCK_STREAM_NAME_4), anyObject(), anyObject())).thenReturn(txnEventStreamWriter4);
        Transaction<Integer> trans4 = mockTransaction();
        UUID txnId4 = UUID.randomUUID();
        Mockito.doReturn(txnId4).when(trans4).getTxnId();
        Mockito.doReturn(trans4).when(txnEventStreamWriter4).beginTxn();
        Mockito.doReturn(trans4).when(txnEventStreamWriter4).getTxn(txnId4);
        transactions.add(trans4);

        TransactionalEventStreamWriter<T> txnEventStreamWriter5 = mockTxnEventStreamWriter();
        Mockito.doReturn(clientFactory).when(writer).createClientFactory(MOCK_SCOPE_NAME_2, MOCK_CLIENT_CONFIG);
        Mockito.when(clientFactory.<T>createTransactionalEventWriter(anyString(), eq(MOCK_STREAM_NAME_5), anyObject(), anyObject())).thenReturn(txnEventStreamWriter5);
        Transaction<Integer> trans5 = mockTransaction();
        UUID txnId5 = UUID.randomUUID();
        Mockito.doReturn(txnId5).when(trans5).getTxnId();
        Mockito.doReturn(trans5).when(txnEventStreamWriter5).beginTxn();
        Mockito.doReturn(trans5).when(txnEventStreamWriter5).getTxn(txnId5);
        transactions.add(trans5);

        return transactions;
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
