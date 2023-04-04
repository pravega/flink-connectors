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

import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.PravegaWriterMode;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PravegaSinkBuilderTest {

    private static final String DEFAULT_SCOPE = "scope1";
    private static final String DEFAULT_STREAM_NAME = "stream1";

    @Test
    public void testValidBuilder() {
        PravegaSinkBuilder<String> builder = new PravegaSinkBuilder<String>()
                .withPravegaConfig(PravegaConfig.fromDefaults().withScope(DEFAULT_SCOPE))
                .forStream(DEFAULT_STREAM_NAME)
                .withSerializationSchema(new SimpleStringSchema());
        PravegaSink<String> sink = builder.build();
        assertThat(sink).isNotNull();
    }

    @Test
    public void testValidBuilderWithWriterMode() {
        PravegaSinkBuilder<String> builder = new PravegaSinkBuilder<String>()
                .withPravegaConfig(PravegaConfig.fromDefaults().withScope(DEFAULT_SCOPE))
                .forStream(DEFAULT_STREAM_NAME)
                .withSerializationSchema(new SimpleStringSchema())
                .withWriterMode(PravegaWriterMode.EXACTLY_ONCE);
        PravegaSink<String> sink = builder.build();
        assertThat(sink).isNotNull();
        assertThat(sink.getClass()).isEqualTo(PravegaTransactionalSink.class);
    }

    @Test
    public void testThrowsExceptionWhenStreamIsNotSet() {
        PravegaSinkBuilder<String> builder = new PravegaSinkBuilder<String>()
                .withPravegaConfig(PravegaConfig.fromDefaults().withScope(DEFAULT_SCOPE))
                .withSerializationSchema(new SimpleStringSchema())
                .withWriterMode(PravegaWriterMode.EXACTLY_ONCE);
        assertThatThrownBy(builder::build).isInstanceOf(IllegalStateException.class);
    }
}