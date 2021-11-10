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
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.connectors.flink.PravegaWriterMode;
import io.pravega.connectors.flink.utils.DirectExecutorService;
import io.pravega.connectors.flink.utils.IntegerSerializationSchema;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;

import static io.pravega.connectors.flink.AbstractStreamingWriterBuilder.DEFAULT_TXN_LEASE_RENEWAL_PERIOD_MILLIS;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class FlinkPravegaInternalWriterTest {
    private static final ClientConfig MOCK_CLIENT_CONFIG = ClientConfig.builder().build();
    private static final String MOCK_SCOPE_NAME = "scope";
    private static final String MOCK_STREAM_NAME = "stream";
    private static final String ROUTING_KEY = "fixed";
    private static final PravegaEventRouter<Integer> FIXED_EVENT_ROUTER = event -> ROUTING_KEY;

    @Test
    @Ignore  // TODO: fix this test!
    public void testConstructor() {
        EventStreamWriter<Integer> pravegaWriter = mockEventStreamWriter();
        PravegaEventRouter<Integer> eventRouter = FIXED_EVENT_ROUTER;
        PravegaWriterMode writerMode = PravegaWriterMode.ATLEAST_ONCE;
        FlinkPravegaInternalWriter<Integer> writer = spyWriter(
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

    // --------- utilities ---------

    @SuppressWarnings("unchecked")
    private <T> EventStreamWriter<T> mockEventStreamWriter() {
        return mock(EventStreamWriter.class);
    }

    private <T> EventStreamClientFactory mockClientFactory(EventStreamWriter<T> eventWriter) {
        EventStreamClientFactory clientFactory = mock(EventStreamClientFactory.class);
        when(clientFactory.<T>createEventWriter(anyString(), anyObject(), anyObject())).thenReturn(eventWriter);
        when(clientFactory.<T>createEventWriter(anyString(), anyString(), anyObject(), anyObject())).thenReturn(eventWriter);
        return clientFactory;
    }

    private FlinkPravegaInternalWriter<Integer> spyWriter(EventStreamClientFactory clientFactory,
                                                        PravegaEventRouter<Integer> eventRouter,
                                                        PravegaWriterMode writerMode) {

        final ExecutorService executorService = spy(new DirectExecutorService());

        FlinkPravegaInternalWriter<Integer> writer = spy(new FlinkPravegaInternalWriter<>(
                MOCK_CLIENT_CONFIG, Stream.of(MOCK_SCOPE_NAME, MOCK_STREAM_NAME), DEFAULT_TXN_LEASE_RENEWAL_PERIOD_MILLIS,
                writerMode, new IntegerSerializationSchema(), eventRouter));

        Mockito.doReturn(executorService).when(writer).createExecutorService();
        Mockito.doReturn(clientFactory).when(writer).createClientFactory(MOCK_SCOPE_NAME, MOCK_CLIENT_CONFIG);

        return writer;
    }
}
