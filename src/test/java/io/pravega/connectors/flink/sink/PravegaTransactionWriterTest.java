///**
// * Copyright Pravega Authors.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package io.pravega.connectors.flink.sink;
//
//import io.grpc.Status;
//import io.grpc.StatusRuntimeException;
//import io.pravega.client.ClientConfig;
//import io.pravega.client.EventStreamClientFactory;
//import io.pravega.client.stream.EventStreamWriter;
//import io.pravega.client.stream.Stream;
//import io.pravega.client.stream.Transaction;
//import io.pravega.client.stream.TransactionalEventStreamWriter;
//import io.pravega.client.stream.TxnFailedException;
//import io.pravega.connectors.flink.PravegaEventRouter;
//import io.pravega.connectors.flink.PravegaWriterMode;
//import io.pravega.connectors.flink.utils.IntegerSerializationSchema;
//import org.apache.flink.api.common.serialization.SerializationSchema;
//import org.apache.flink.api.common.typeutils.base.IntSerializer;
//import org.apache.flink.api.connector.sink.Sink;
//import org.apache.flink.api.connector.sink.SinkWriter;
//import org.apache.flink.streaming.runtime.operators.sink.SinkOperatorFactory;
//import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
//import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
//import org.junit.Assert;
//import org.junit.Test;
//import org.mockito.Mockito;
//
//import javax.annotation.Nullable;
//import java.io.IOException;
//import java.util.Optional;
//import java.util.UUID;
//
//import static io.pravega.connectors.flink.sink.PravegaSinkBuilder.DEFAULT_TXN_LEASE_RENEWAL_PERIOD_MILLIS;
//import static org.mockito.Matchers.any;
//import static org.mockito.Matchers.anyObject;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.never;
//import static org.mockito.Mockito.spy;
//import static org.mockito.Mockito.verify;
//
//public class PravegaTransactionWriterTest {
//    private static final ClientConfig MOCK_CLIENT_CONFIG = ClientConfig.builder().build();
//    private static final String MOCK_SCOPE_NAME = "scope";
//    private static final String MOCK_STREAM_NAME = "stream";
//    private static final String ROUTING_KEY = "fixed";
//    private static final PravegaEventRouter<Integer> FIXED_EVENT_ROUTER = event -> ROUTING_KEY;
//
//    /**
//     * Tests the constructor.
//     */
//    @Test
//    public void testConstructor() {
//        final TestablePravegaTransactionWriter<Integer> writer = new TestablePravegaTransactionWriter<>(
//                new IntegerSerializationSchema());
//        assert writer.getEventRouter() != null;
//        Assert.assertEquals(FIXED_EVENT_ROUTER.getRoutingKey(1), writer.getEventRouter().getRoutingKey(1));
//        // both internal writer and transaction should be set up
//        Assert.assertNotNull(writer.getInternalWriter());
//        Assert.assertNotNull(writer.getTransactionId());
//    }
//
//    /**
//     * Tests the {@code processElement} method.
//     */
//    @Test
//    public void testTransactionalWriterWrite() throws Exception {
//        final TestablePravegaTransactionWriter<Integer> writer = new TestablePravegaTransactionWriter<>(
//                new IntegerSerializationSchema());
//        final TestablePravegaCommitter<Integer> committer = new TestablePravegaCommitter<>(
//                new IntegerSerializationSchema());
//        final Transaction<Integer> trans = writer.trans;
//        final Transaction<Integer> reconstructedTrans = mockTransaction();
//
//        Mockito.doAnswer(ans -> {
//            // update the exposed trans with the latest resumed txnId
//            UUID txnId = ans.getArgument(0, UUID.class);
//            Mockito.doReturn(txnId).when(reconstructedTrans).getTxnId();
//            // mock it ready for the commit
//            Mockito.doReturn(Transaction.Status.OPEN).when(reconstructedTrans).checkStatus();
//            return reconstructedTrans;
//        }).when(committer.transactionalWriter).getTxn(any(UUID.class));
//
//        try (OneInputStreamOperatorTestHarness<Integer, byte[]> testHarness =
//                     createTestHarness(writer, committer)) {
//            testHarness.open();
//            StreamRecord<Integer> e1 = new StreamRecord<>(1, 1L);
//            testHarness.processElement(e1);
//            verify(trans).writeEvent(ROUTING_KEY, e1.getValue());
//
//            // verify the prepareCommit is called and events are flushed
//            testHarness.prepareSnapshotPreBarrier(1L);
//            verify(trans).flush();
//
//            // trigger the internal process to save the committables
//            testHarness.snapshot(1L, 3L);
//
//            // call the committer to reconstruct the trans and commit them
//            testHarness.notifyOfCompletedCheckpoint(1L);
//            Assert.assertEquals(reconstructedTrans.getTxnId(), trans.getTxnId());
//            verify(reconstructedTrans).commit();
//        }
//    }
//
//    /**
//     * Tests the error handling if it fails at {@code writeEvent}.
//     */
//    @Test
//    public void testTransactionalWriterWriteFail() throws Exception {
//        final TestablePravegaTransactionWriter<Integer> writer = new TestablePravegaTransactionWriter<>(
//                new IntegerSerializationSchema());
//        final TestablePravegaCommitter<Integer> committer = new TestablePravegaCommitter<>(
//                new IntegerSerializationSchema());
//        final Transaction<Integer> trans = writer.trans;
//
//        Mockito.doThrow(new TxnFailedException()).when(trans).writeEvent(anyObject(), anyObject());
//
//        try (OneInputStreamOperatorTestHarness<Integer, byte[]> testHarness =
//                     createTestHarness(writer, committer)) {
//            testHarness.open();
//            StreamRecord<Integer> e1 = new StreamRecord<>(1, 1L);
//
//            try {
//                testHarness.processElement(e1);
//                Assert.fail("Expected a TxnFailedException wrapped in IOException");
//            } catch (IOException e) {
//                // TxnFailedException wrapped in IOException is caught
//            }
//        }
//    }
//
//    /**
//     * Tests the error handling if it fails at {@code flush}.
//     */
//    @Test
//    public void testTransactionalWriterPrepareCommitFail() throws Exception {
//        final TestablePravegaTransactionWriter<Integer> writer = new TestablePravegaTransactionWriter<>(
//                new IntegerSerializationSchema());
//        final TestablePravegaCommitter<Integer> committer = new TestablePravegaCommitter<>(
//                new IntegerSerializationSchema());
//        final Transaction<Integer> trans = writer.trans;
//
//        Mockito.doThrow(new TxnFailedException()).when(trans).flush();
//
//        try (OneInputStreamOperatorTestHarness<Integer, byte[]> testHarness =
//                     createTestHarness(writer, committer)) {
//            testHarness.open();
//            StreamRecord<Integer> e1 = new StreamRecord<>(1, 1L);
//            testHarness.processElement(e1);
//            verify(trans).writeEvent(ROUTING_KEY, e1.getValue());
//
//            try {
//                // call the prepareCommit
//                testHarness.prepareSnapshotPreBarrier(1L);
//                Assert.fail("Expected a TxnFailedException wrapped in IOException");
//            } catch (IOException e) {
//                // TxnFailedException wrapped in IOException is caught
//            }
//        }
//    }
//
//    /**
//     * Tests the error handling with unknown transaction.
//     */
//    @Test
//    public void testTransactionalWriterCommitWithUnknownId() throws Exception {
//        final TestablePravegaTransactionWriter<Integer> writer = new TestablePravegaTransactionWriter<>(
//                new IntegerSerializationSchema());
//        final TestablePravegaCommitter<Integer> committer = new TestablePravegaCommitter<>(
//                new IntegerSerializationSchema());
//
//        Mockito.doAnswer(ans -> {
//            final Transaction<Integer> reconstructedTrans = mockTransaction();
//            // update the exposed trans with the latest resumed txnId
//            UUID txnId = ans.getArgument(0, UUID.class);
//            Mockito.doReturn(txnId).when(reconstructedTrans).getTxnId();
//            // mock it with unknown id exception
//            Mockito.when(reconstructedTrans.checkStatus()).thenThrow(new StatusRuntimeException(Status.NOT_FOUND));
//            return reconstructedTrans;
//        }).when(committer.transactionalWriter).getTxn(any(UUID.class));
//
//        try (OneInputStreamOperatorTestHarness<Integer, byte[]> testHarness =
//                     createTestHarness(writer, committer)) {
//            testHarness.open();
//            StreamRecord<Integer> e1 = new StreamRecord<>(1, 1L);
//            testHarness.processElement(e1);
//            testHarness.prepareSnapshotPreBarrier(1L);
//            testHarness.snapshot(1L, 3L);
//            testHarness.notifyOfCompletedCheckpoint(1L);
//            // StatusRuntimeException with Unknown transaction is caught
//        }
//    }
//
//    /**
//     * Tests the error handling.
//     */
//    @Test
//    public void testTransactionalWriterCommitFail() throws Exception {
//        final TestablePravegaTransactionWriter<Integer> writer = new TestablePravegaTransactionWriter<>(
//                new IntegerSerializationSchema());
//        final TestablePravegaCommitter<Integer> committer = new TestablePravegaCommitter<>(
//                new IntegerSerializationSchema());
//
//        Mockito.doAnswer(ans -> {
//            final Transaction<Integer> reconstructedTrans = mockTransaction();
//            // update the exposed trans with the latest resumed txnId
//            UUID txnId = ans.getArgument(0, UUID.class);
//            Mockito.doReturn(txnId).when(reconstructedTrans).getTxnId();
//            // mock it ready for commit
//            Mockito.when(reconstructedTrans.checkStatus()).thenReturn(Transaction.Status.OPEN);
//            // but fail to commit
//            Mockito.doThrow(new TxnFailedException()).when(reconstructedTrans).commit();
//            return reconstructedTrans;
//        }).when(committer.transactionalWriter).getTxn(any(UUID.class));
//
//        try (OneInputStreamOperatorTestHarness<Integer, byte[]> testHarness =
//                     createTestHarness(writer, committer)) {
//            testHarness.open();
//            StreamRecord<Integer> e1 = new StreamRecord<>(1, 1L);
//            testHarness.processElement(e1);
//            testHarness.prepareSnapshotPreBarrier(1L);
//            testHarness.snapshot(1L, 3L);
//            testHarness.notifyOfCompletedCheckpoint(1L);
//            // TxnFailedException is caught
//        }
//    }
//
//    /**
//     * Tests the wrong transaction status while committing.
//     */
//    @Test
//    public void testTransactionalWriterCommitWithWrongStatus() throws Exception {
//        final TestablePravegaTransactionWriter<Integer> writer = new TestablePravegaTransactionWriter<>(
//                new IntegerSerializationSchema());
//        final TestablePravegaCommitter<Integer> committer = new TestablePravegaCommitter<>(
//                new IntegerSerializationSchema());
//        final Transaction<Integer> reconstructedTrans = mockTransaction();
//
//        Mockito.doAnswer(ans -> {
//            // update the exposed trans with the latest resumed txnId
//            UUID txnId = ans.getArgument(0, UUID.class);
//            Mockito.doReturn(txnId).when(reconstructedTrans).getTxnId();
//            // mock the transaction with aborted status
//            Mockito.when(reconstructedTrans.checkStatus()).thenReturn(Transaction.Status.ABORTED);
//            return reconstructedTrans;
//        }).when(committer.transactionalWriter).getTxn(any(UUID.class));
//
//        try (OneInputStreamOperatorTestHarness<Integer, byte[]> testHarness =
//                     createTestHarness(writer, committer)) {
//            testHarness.open();
//            StreamRecord<Integer> e1 = new StreamRecord<>(1, 1L);
//            testHarness.processElement(e1);
//            testHarness.prepareSnapshotPreBarrier(1L);
//            testHarness.snapshot(1L, 3L);
//            testHarness.notifyOfCompletedCheckpoint(1L);
//
//            verify(reconstructedTrans, never()).commit();
//        }
//    }
//
//    /**
//     * Tests the {@code close} method.
//     */
//    @SuppressWarnings("unchecked")
//    @Test
//    public void testTransactionalWriterClose() throws Exception {
//        final TestablePravegaTransactionWriter<Integer> writer = new TestablePravegaTransactionWriter<>(
//                new IntegerSerializationSchema());
//        final TestablePravegaCommitter<Integer> committer = new TestablePravegaCommitter<>(
//                new IntegerSerializationSchema());
//        final Transaction<Integer> trans = writer.trans;
//        final TransactionalEventStreamWriter<Integer> txnEventStreamWriter = writer.getInternalWriter();
//
//        Mockito.when(trans.checkStatus()).thenReturn(Transaction.Status.OPEN);
//
//        try {
//            try (OneInputStreamOperatorTestHarness<Integer, byte[]> testHarness =
//                         createTestHarness(writer, committer)) {
//                testHarness.open();
//                assert txnEventStreamWriter != null;
//
//                // prepare a worst-case situation that exercises the exception handling aspect of close
//                Mockito.doThrow(new IntentionalRuntimeException()).when(txnEventStreamWriter).close();
//            }
//        } catch (Exception e) {
//            Assert.assertTrue(e instanceof IntentionalRuntimeException);
//        }
//    }
//
//    public static class TestablePravegaTransactionWriter<T> extends PravegaTransactionWriter<T> {
//        Transaction<T> trans;  // the mocked transaction that should replace the PravegaTransactionWriter#transaction
//        UUID txnId;
//
//        public TestablePravegaTransactionWriter(SerializationSchema<T> serializationSchema) {
//            super(mock(Sink.InitContext.class), MOCK_CLIENT_CONFIG, Stream.of(MOCK_SCOPE_NAME, MOCK_STREAM_NAME),
//                    DEFAULT_TXN_LEASE_RENEWAL_PERIOD_MILLIS, serializationSchema, event -> ROUTING_KEY);
//        }
//
//        @Override
//        protected TransactionalEventStreamWriter<T> initializeInternalWriter() {
//            trans = mockTransaction();
//            txnId = UUID.randomUUID();
//
//            TransactionalEventStreamWriter<T> pravegaTxnWriter = mockTxnEventStreamWriter();
//            Mockito.doReturn(txnId).when(trans).getTxnId();
//            Mockito.doReturn(trans).when(pravegaTxnWriter).beginTxn();
//            Mockito.doReturn(Transaction.Status.OPEN).when(trans).checkStatus();
//
//            clientFactory = mock(EventStreamClientFactory.class);
//
//            return pravegaTxnWriter;
//        }
//    }
//
//    public static class TestablePravegaCommitter<T> extends PravegaCommitter<T> {
//        public TestablePravegaCommitter(SerializationSchema<T> serializationSchema) {
//            super(MOCK_CLIENT_CONFIG, Stream.of(MOCK_SCOPE_NAME, MOCK_STREAM_NAME),
//                    DEFAULT_TXN_LEASE_RENEWAL_PERIOD_MILLIS, serializationSchema);
//        }
//
//        @Override
//        protected TransactionalEventStreamWriter<T> initializeInternalWriter() {
//            return mockTxnEventStreamWriter();
//        }
//    }
//
//    @SuppressWarnings("unchecked")
//    private static <T> Transaction<T> mockTransaction() {
//        return mock(Transaction.class);
//    }
//
//    @SuppressWarnings("unchecked")
//    private static <T> TransactionalEventStreamWriter<T> mockTxnEventStreamWriter() {
//        return mock(TransactionalEventStreamWriter.class);
//    }
//
//    private PravegaSink<Integer> mockSink(PravegaWriterMode writerMode,
//                                          SinkWriter<Integer, PravegaTransactionState, Void> writer,
//                                          @Nullable PravegaCommitter<Integer> committer) throws IOException {
//        final PravegaSink<Integer> sink = spy(new PravegaSink<>(false, MOCK_CLIENT_CONFIG,
//                Stream.of(MOCK_SCOPE_NAME, MOCK_STREAM_NAME), DEFAULT_TXN_LEASE_RENEWAL_PERIOD_MILLIS,
//                writerMode, new IntegerSerializationSchema(), FIXED_EVENT_ROUTER));
//
//        Mockito.doReturn(writer).when(sink).createWriter(anyObject(), anyObject());
//        Mockito.doReturn(committer != null ? Optional.of(committer) : Optional.empty()).when(sink).createCommitter();
//
//        return sink;
//    }
//
//    /**
//     * A test harness suitable for EXACTLY_ONCE tests.
//     *
//     * @param writer An internal writer that contains {@link EventStreamWriter}.
//     * @param committer A committer that commit the reconstructed transaction.
//     * @return A test harness.
//     */
//    private OneInputStreamOperatorTestHarness<Integer, byte[]> createTestHarness(PravegaTransactionWriter<Integer> writer,
//                                                                                 PravegaCommitter<Integer> committer) throws Exception {
//        return new OneInputStreamOperatorTestHarness<>(
//                new SinkOperatorFactory<>(
//                        mockSink(PravegaWriterMode.EXACTLY_ONCE, writer, committer), false, true),
//                IntSerializer.INSTANCE);
//    }
//
//    private static class IntentionalRuntimeException extends RuntimeException {
//    }
//}
