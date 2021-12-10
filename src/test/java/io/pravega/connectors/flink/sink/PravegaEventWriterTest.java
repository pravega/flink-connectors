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
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

import static io.pravega.connectors.flink.sink.PravegaSinkBuilder.DEFAULT_TXN_LEASE_RENEWAL_PERIOD_MILLIS;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PravegaEventWriterTest {

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
        final TestablePravegaEventWriter<Integer> writer = new TestablePravegaEventWriter<>(
                new IntegerSerializationSchema());
        assert writer.getEventRouter() != null;
        Assert.assertEquals(FIXED_EVENT_ROUTER.getRoutingKey(1), writer.getEventRouter().getRoutingKey(1));
        Assert.assertEquals(PravegaWriterMode.ATLEAST_ONCE, writer.getWriterMode());
        Assert.assertNotNull(writer.getInternalWriter());
        Assert.assertNotNull(writer.executorService);
        Assert.assertNotNull(writer.clientFactory);
    }

    /**
     * Tests the internal serializer.
     */
    @Test
    public void testFlinkSerializer() {
        IntegerSerializationSchema schema = new IntegerSerializationSchema();
        FlinkSerializer<Integer> serializer = new FlinkSerializer<>(schema);
        Integer val = 42;
        Assert.assertEquals(ByteBuffer.wrap(schema.serialize(val)), serializer.serialize(val));
        try {
            serializer.deserialize(ByteBuffer.wrap(schema.serialize(val)));
            Assert.fail("expected an exception");
        } catch (IllegalStateException e) {
            // expected
        }
    }

    /**
     * Tests the {@code processElement} method.
     * See also: {@code testNonTransactionalWriterProcessElementAccounting}, {@code testNonTransactionalWriterProcessElementErrorHandling}
     */
    @Test
    public void testNonTransactionalWriterWriting() throws Exception {
        final TestablePravegaEventWriter<Integer> writer = new TestablePravegaEventWriter<>(
                new IntegerSerializationSchema());
        final EventStreamWriter<Integer> eventStreamWriter = writer.getInternalWriter();

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
        final TestablePravegaEventWriter<Integer> writer = new TestablePravegaEventWriter<>(
                new IntegerSerializationSchema());
        final EventStreamWriter<Integer> eventStreamWriter = writer.getInternalWriter();

        try (OneInputStreamOperatorTestHarness<Integer, byte[]> testHarness =
                     createTestHarness(writer)) {
            testHarness.open();
            assert eventStreamWriter != null;

            CompletableFuture<Void> e1Future = new CompletableFuture<>();
            when(eventStreamWriter.writeEvent(anyString(), anyObject())).thenReturn(e1Future);
            StreamRecord<Integer> e1 = new StreamRecord<>(1, 1L);
            testHarness.processElement(e1);
            Assert.assertEquals(1, writer.pendingWritesCount.get());

            CompletableFuture<Void> e2Future = new CompletableFuture<>();
            when(eventStreamWriter.writeEvent(anyString(), anyObject())).thenReturn(e2Future);
            StreamRecord<Integer> e2 = new StreamRecord<>(2, 2L);
            testHarness.processElement(e2);
            Assert.assertEquals(2, writer.pendingWritesCount.get());

            CompletableFuture<Void> e3Future = new CompletableFuture<>();
            when(eventStreamWriter.writeEvent(anyString(), anyObject())).thenReturn(e3Future);
            StreamRecord<Integer> e3 = new StreamRecord<>(3, 3L);
            testHarness.processElement(e3);
            Assert.assertEquals(3, writer.pendingWritesCount.get());

            e1Future.complete(null);
            e2Future.completeExceptionally(new IntentionalRuntimeException());
            e3Future.complete(null);
            Assert.assertEquals(0, writer.pendingWritesCount.get());

            // clear the error for test simplicity
            writer.writeError.set(null);
        }
    }

    /**
     * Tests the handling of write errors.
     */
    @Test
    public void testNonTransactionalWriterProcessElementErrorHandling() throws Exception {
        final TestablePravegaEventWriter<Integer> writer = new TestablePravegaEventWriter<>(
                new IntegerSerializationSchema());
        final EventStreamWriter<Integer> eventStreamWriter = writer.getInternalWriter();

        try (OneInputStreamOperatorTestHarness<Integer, byte[]> testHarness =
                     createTestHarness(writer)) {
            testHarness.open();
            assert eventStreamWriter != null;

            CompletableFuture<Void> e1Future = new CompletableFuture<>();
            when(eventStreamWriter.writeEvent(anyString(), anyObject())).thenReturn(e1Future);
            StreamRecord<Integer> e1 = new StreamRecord<>(1, 1L);
            testHarness.processElement(e1);
            e1Future.completeExceptionally(new IntentionalRuntimeException());
            Assert.assertNotNull(writer.writeError.get());

            StreamRecord<Integer> e2 = new StreamRecord<>(2, 2L);
            try {
                testHarness.processElement(e2);
                Assert.fail("expected an IOException due to a prior write error");
            } catch (IOException e) {
                // expected IOException due to a prior write error
            }

            // clear the error for test simplicity
            writer.writeError.set(null);
        }
    }

    /**
     * Tests the handling of flushes, which occur upon snapshot and close.
     */
    @Test
    public void testNonTransactionalWriterFlush() throws Exception {
        final TestablePravegaEventWriter<Integer> writer = new TestablePravegaEventWriter<>(
                new IntegerSerializationSchema());
        final EventStreamWriter<Integer> eventStreamWriter = writer.getInternalWriter();

        try (OneInputStreamOperatorTestHarness<Integer, byte[]> testHarness =
                     createTestHarness(writer)) {
            testHarness.open();
            assert eventStreamWriter != null;

            // invoke a flush, expecting it to block on pending writes
            writer.pendingWritesCount.incrementAndGet();
            Future<Void> flushFuture = runAsync(writer::flushAndVerify);
            Thread.sleep(1000);
            Assert.assertFalse(flushFuture.isDone());

            // allow the flush to complete
            synchronized (writer) {
                writer.pendingWritesCount.decrementAndGet();
                writer.notify();
            }
            flushFuture.get();

            // invoke another flush following a write error, expecting failure
            writer.writeError.set(new IntentionalRuntimeException());
            try {
                writer.flushAndVerify();
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
        final TestablePravegaEventWriter<Integer> writer = new TestablePravegaEventWriter<>(
                new IntegerSerializationSchema());
        final EventStreamWriter<Integer> eventStreamWriter = writer.getInternalWriter();

        try {
            try (OneInputStreamOperatorTestHarness<Integer, byte[]> testHarness =
                         createTestHarness(writer)) {
                testHarness.open();
                assert eventStreamWriter != null;

                // prepare a worst-case situation that exercises the exception handling aspect of close
                writer.writeError.set(new IntentionalRuntimeException());
                Mockito.doThrow(new IntentionalRuntimeException()).when(eventStreamWriter).close();
                Mockito.doThrow(new IntentionalRuntimeException())
                        .when(writer.executorService)
                        .shutdown();
            }
        } catch (IOException e) {
            Assert.assertEquals(2, e.getSuppressed().length);
            Assert.assertTrue(e.getSuppressed()[0] instanceof IntentionalRuntimeException);
            Assert.assertTrue(e.getSuppressed()[1] instanceof IntentionalRuntimeException);
        }
    }

    private static class TestablePravegaEventWriter<T> extends PravegaEventWriter<T> {

        public TestablePravegaEventWriter(SerializationSchema<T> serializationSchema) {
            super(mock(Sink.InitContext.class), MOCK_CLIENT_CONFIG, Stream.of(MOCK_SCOPE_NAME, MOCK_STREAM_NAME),
                    PravegaWriterMode.ATLEAST_ONCE, serializationSchema, event -> ROUTING_KEY);
        }

        @Override
        protected EventStreamWriter<T> initializeInternalWriter() {
            clientFactory = mock(EventStreamClientFactory.class);
            executorService = spy(new DirectExecutorService());
            return mockEventStreamWriter();
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> EventStreamWriter<T> mockEventStreamWriter() {
        return mock(EventStreamWriter.class);
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

    /**
     * A test harness suitable for ATLEAST_ONCE tests.
     *
     * @param writer An internal writer that contains {@link EventStreamWriter}.
     * @return A test harness.
     */
    private OneInputStreamOperatorTestHarness<Integer, byte[]> createTestHarness(PravegaEventWriter<Integer> writer) throws Exception {
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
}
