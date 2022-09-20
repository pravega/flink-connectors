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

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.utils.DirectExecutorService;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit Test for the {@link FlinkPravegaOutputFormat}
 */
public class FlinkPravegaOutputFormatTest {

    private final SerializationSchema<String> serializationSchema = mock(SerializationSchema.class);

    private final PravegaConfig pravegaConfig = mock(PravegaConfig.class);

    private final PravegaEventRouter<String> eventRouter = new FixedEventRouter<>();

    private final Stream stream = Stream.of("test", "foo");

    private final ClientConfig clientConfig = ClientConfig.builder().build();

    /**
     * Testing the builder for right configurations
     */
    @Test
    public void testBuilderForSuccess() {
        when(pravegaConfig.getClientConfig()).thenReturn(clientConfig);
        when(pravegaConfig.resolve(anyString())).thenReturn(stream);
        FlinkPravegaOutputFormat outputFormat = FlinkPravegaOutputFormat.<String>builder()
                .withEventRouter(eventRouter)
                .withSerializationSchema(serializationSchema)
                .withPravegaConfig(pravegaConfig)
                .forStream(stream)
                .build();
        assertThat(outputFormat.getScope()).isEqualTo(stream.getScope());
        assertThat(outputFormat.getStream()).isEqualTo(stream.getStreamName());
        assertThat(outputFormat.getSerializationSchema()).isEqualTo(serializationSchema);
        assertThat(outputFormat.getEventRouter()).isEqualTo(eventRouter);
    }

    /**
     * Testing the builder for right configurations.
     * Should fail since we don't pass {@link SerializationSchema}
     */
    @Test
    public void testBuilderForFailure1() {
        PravegaConfig pravegaConfig = mock(PravegaConfig.class);
        assertThatThrownBy(
                () -> FlinkPravegaOutputFormat.<String>builder()
                        .withEventRouter(eventRouter)
                        .withPravegaConfig(pravegaConfig)
                        .forStream(stream)
                        .build())
                .isInstanceOf(NullPointerException.class);
    }

    /**
     * Testing the builder for right configurations.
     * Should fail since we don't pass {@link Stream}
     */
    @Test
    public void testBuilderForFailure2() {
        assertThatThrownBy(
                () -> FlinkPravegaOutputFormat.<String>builder()
                        .withEventRouter(eventRouter)
                        .withSerializationSchema(serializationSchema)
                        .withPravegaConfig(pravegaConfig)
                        .build())
                .isInstanceOf(IllegalStateException.class);
    }

    /**
     * Test case to validate the lifecycle methods of {@link OutputFormat}
     *
     * @throws Exception
     */
    @Test
    public void testLifecycleMethods() throws Exception  {
        ClientConfig clientConfig = ClientConfig.builder().build();
        when(pravegaConfig.getClientConfig()).thenReturn(clientConfig);
        when(pravegaConfig.resolve(anyString())).thenReturn(stream);

        EventStreamWriter<String> pravegaWriter = mockEventStreamWriter();
        EventStreamClientFactory clientFactory = mockClientFactory(pravegaWriter);
        FlinkPravegaOutputFormat<String> spyFlinkPravegaOutputFormat = spyFlinkPravegaOutputFormat(clientFactory);

        // test open
        spyFlinkPravegaOutputFormat.open(0, 1);
        verify(spyFlinkPravegaOutputFormat).open(0, 1);

        CompletableFuture<Void> writeFuture = new CompletableFuture<>();
        when(pravegaWriter.writeEvent(anyString(), anyObject())).thenReturn(writeFuture);

        ExecutorService executorService = spy(new DirectExecutorService());
        Mockito.doReturn(executorService).when(spyFlinkPravegaOutputFormat).createExecutorService();

        // test writeRecord success
        spyFlinkPravegaOutputFormat.writeRecord("test-1");
        assertThat(spyFlinkPravegaOutputFormat.getPendingWritesCount().get()).isEqualTo(1);
        writeFuture.complete(null);

        // test writeRecord induce failure
        spyFlinkPravegaOutputFormat.writeRecord("test-2");
        writeFuture.completeExceptionally(new Exception("test simulated"));

        // test writeRecord after a failure
        try {
            spyFlinkPravegaOutputFormat.writeRecord("test-3");
        } catch (Exception e) {
            assertThat(e instanceof IOException).isTrue();
            assertThat(spyFlinkPravegaOutputFormat.isErrorOccurred()).isTrue();
            assertThat(e.getCause().getMessage()).isEqualTo("test simulated");
            assertThat(spyFlinkPravegaOutputFormat.getPendingWritesCount().get()).isEqualTo(0);
        }

        // test close error
        try {
            spyFlinkPravegaOutputFormat.close();
        } catch (Exception e) {
            assertThat(e instanceof IOException).isTrue();
        }

        // test close
        reset(clientFactory);
        reset(spyFlinkPravegaOutputFormat);
        spyFlinkPravegaOutputFormat.close();
        verify(clientFactory).close();
    }

    @Test
    public void testSchemaRegistrySerialization() throws Exception {
        PravegaConfig pravegaConfig = PravegaConfig.fromDefaults();
        try {
            FlinkPravegaOutputFormat.<Integer>builder()
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
            FlinkPravegaOutputFormat.<Integer>builder()
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

    private static class FixedEventRouter<T> implements PravegaEventRouter<T> {
        @Override
        public String getRoutingKey(T event) {
            return "fixed";
        }
    }

    @SuppressWarnings("unchecked")
    private <T> EventStreamWriter<T> mockEventStreamWriter() {
        return mock(EventStreamWriter.class);
    }

    private <T> EventStreamClientFactory mockClientFactory(EventStreamWriter<T> eventWriter) {
        EventStreamClientFactory clientFactory = mock(EventStreamClientFactory.class);
        when(clientFactory.<T>createEventWriter(anyString(), anyObject(), anyObject())).thenReturn(eventWriter);
        return clientFactory;
    }

    private FlinkPravegaOutputFormat<String> spyFlinkPravegaOutputFormat(EventStreamClientFactory clientFactory) {

        FlinkPravegaOutputFormat<String> flinkPravegaOutputFormat = FlinkPravegaOutputFormat.<String>builder()
                .withEventRouter(eventRouter)
                .withSerializationSchema(serializationSchema)
                .withPravegaConfig(pravegaConfig)
                .forStream(stream)
                .build();

        FlinkPravegaOutputFormat<String> spyFlinkPravegaOutputFormat = spy(flinkPravegaOutputFormat);
        doReturn(clientFactory).when(spyFlinkPravegaOutputFormat).createClientFactory(anyString(), any());
        return spyFlinkPravegaOutputFormat;
    }

}
