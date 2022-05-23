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

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.serialization.DeserializerFromSchemaRegistry;
import io.pravega.connectors.flink.utils.SchemaRegistryUtils;
import io.pravega.connectors.flink.utils.SetupUtils;
import io.pravega.connectors.flink.utils.User;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.json.schemas.JSONSchema;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class FlinkPravegaSchemaRegistryWriterTestITCase {

    private static class MyTest {
        public String a;

        public MyTest() {
        }

        public MyTest(String a) {
            this.a = a;
        }

        public String getA() {
            return a;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MyTest myTest = (MyTest) o;
            return Objects.equals(a, myTest.a);
        }

        @Override
        public int hashCode() {
            return Objects.hash(a);
        }
    }

    // Setup utility.
    protected static final SetupUtils SETUP_UTILS = new SetupUtils();

    protected static final SchemaRegistryUtils SCHEMA_REGISTRY_UTILS =
            new SchemaRegistryUtils(SETUP_UTILS, SchemaRegistryUtils.DEFAULT_PORT);

    private static final Schema SCHEMA = User.SCHEMA$;
    private static final GenericRecord AVRO_GEN_EVENT = new GenericRecordBuilder(SCHEMA).set("name", "test").build();
    private static final User AVRO_SPEC_EVENT = User.newBuilder().setName("test").build();
    private static final MyTest JSON_EVENT = new MyTest("test");

    //Ensure each test completes within 180 seconds.
    @Rule
    public final Timeout globalTimeout = new Timeout(180, TimeUnit.SECONDS);

    @BeforeClass
    public static void setupServices() throws Exception {
        SETUP_UTILS.startAllServices();
        SCHEMA_REGISTRY_UTILS.setupServices();
    }

    @AfterClass
    public static void tearDownServices() throws Exception {
        SETUP_UTILS.stopAllServices();
        SCHEMA_REGISTRY_UTILS.tearDownServices();
    }

    @Test
    public void testWriterWithAvroGenericRecordRegistrySerializer() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        preparePravegaStream(streamName, AvroSchema.of(SCHEMA));

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkPravegaWriter<GenericRecord> writer = FlinkPravegaWriter.<GenericRecord>builder()
                .forStream(streamName)
                .enableMetrics(false)
                .withPravegaConfig(SETUP_UTILS.getPravegaConfig().withSchemaRegistryURI(SCHEMA_REGISTRY_UTILS.getSchemaRegistryUri()))
                .withSerializationSchemaFromRegistry(streamName, GenericRecord.class)
                .build();

        env.addSource(new SourceFunction<GenericRecord>() {
            @Override
            public void run(SourceContext<GenericRecord> ctx) throws Exception {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(AVRO_GEN_EVENT);
                }
            }

            @Override
            public void cancel() {
            }
        }, new GenericRecordAvroTypeInfo(SCHEMA)).addSink(writer);

        try {
            env.execute("Schema Registry Read");
        } catch (Exception e) {
            throw e;
        }

        EventStreamReader<GenericRecord> reader = getReader(streamName, GenericRecord.class);
        final EventRead<GenericRecord> eventRead = reader.readNextEvent(1000);
        final GenericRecord event = eventRead.getEvent();

        Assert.assertEquals(event, AVRO_GEN_EVENT);
    }

    @Test
    public void testWriterWithAvroSpecificRecordRegistrySerializer() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        preparePravegaStream(streamName, AvroSchema.of(SCHEMA));

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkPravegaWriter<User> writer = FlinkPravegaWriter.<User>builder()
                .forStream(streamName)
                .enableMetrics(false)
                .withPravegaConfig(SETUP_UTILS.getPravegaConfig().withSchemaRegistryURI(SCHEMA_REGISTRY_UTILS.getSchemaRegistryUri()))
                .withSerializationSchemaFromRegistry(streamName, User.class)
                .build();

        env.addSource(new SourceFunction<User>() {
            @Override
            public void run(SourceContext<User> ctx) throws Exception {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(AVRO_SPEC_EVENT);
                }
            }

            @Override
            public void cancel() {
            }
        }).addSink(writer);

        try {
            env.execute("Schema Registry Read");
        } catch (Exception e) {
            throw e;
        }

        EventStreamReader<User> reader = getReader(streamName, User.class);
        final EventRead<User> eventRead = reader.readNextEvent(1000);
        final User event = eventRead.getEvent();

        Assert.assertEquals(event, AVRO_SPEC_EVENT);
    }

    @Test
    public void testWriterWithJsonRegistrySerializer() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        preparePravegaStream(streamName, JSONSchema.of(MyTest.class));

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkPravegaWriter<MyTest> writer = FlinkPravegaWriter.<MyTest>builder()
                .forStream(streamName)
                .enableMetrics(false)
                .withPravegaConfig(SETUP_UTILS.getPravegaConfig().withSchemaRegistryURI(SCHEMA_REGISTRY_UTILS.getSchemaRegistryUri()))
                .withSerializationSchemaFromRegistry(streamName, MyTest.class)
                .build();

        env.addSource(new SourceFunction<MyTest>() {
            @Override
            public void run(SourceContext<MyTest> ctx) throws Exception {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(JSON_EVENT);
                }
            }

            @Override
            public void cancel() {
            }
        }).addSink(writer);

        try {
            env.execute("Schema Registry Read");
        } catch (Exception e) {
            throw e;
        }

        EventStreamReader<MyTest> reader = getReader(streamName, MyTest.class);
        final EventRead<MyTest> eventRead = reader.readNextEvent(1000);
        final MyTest event = eventRead.getEvent();

        Assert.assertEquals(event, JSON_EVENT);
    }

    @Test
    public void testOutputFormatWithAvroRegistrySerializer() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        preparePravegaStream(streamName, AvroSchema.of(SCHEMA));

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        FlinkPravegaOutputFormat<GenericRecord> writer = FlinkPravegaOutputFormat.<GenericRecord>builder()
                .forStream(streamName)
                .enableMetrics(false)
                .withPravegaConfig(SETUP_UTILS.getPravegaConfig().withSchemaRegistryURI(SCHEMA_REGISTRY_UTILS.getSchemaRegistryUri()))
                .withSerializationSchemaFromRegistry(streamName, GenericRecord.class)
                .build();

        env.fromElements(AVRO_GEN_EVENT)
                .output(writer);

        try {
            env.execute("Schema Registry Read");
        } catch (Exception e) {
            throw e;
        }

        EventStreamReader<GenericRecord> reader = getReader(streamName, GenericRecord.class);
        final EventRead<GenericRecord> eventRead = reader.readNextEvent(1000);
        final GenericRecord event = eventRead.getEvent();

        Assert.assertEquals(event, AVRO_GEN_EVENT);
    }

    // ================================================================================

    private void preparePravegaStream(String streamName, io.pravega.schemaregistry.serializer.shared.schemas.Schema schema) throws Exception {
        SETUP_UTILS.createTestStream(streamName, 1);
        SCHEMA_REGISTRY_UTILS.registerSchema(streamName, schema, schema.getSchemaInfo().getSerializationFormat());
    }

    private <T> EventStreamReader<T> getReader(String streamName, Class<T> tClass) {
        final String readerGroupName = RandomStringUtils.randomAlphabetic(20);
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SETUP_UTILS.getScope(), SETUP_UTILS.getClientConfig())) {
            readerGroupManager.createReaderGroup(
                    readerGroupName,
                    ReaderGroupConfig.builder().stream(Stream.of(SETUP_UTILS.getScope(), streamName)).build());
        }

        return EventStreamClientFactory.withScope(SETUP_UTILS.getScope(), SETUP_UTILS.getClientConfig()).createReader(
                RandomStringUtils.randomAlphabetic(20),
                readerGroupName,
                new DeserializerFromSchemaRegistry<>(SETUP_UTILS.getPravegaConfig().withSchemaRegistryURI(
                        SCHEMA_REGISTRY_UTILS.getSchemaRegistryUri()), streamName, tClass),
                ReaderConfig.builder().build());
    }
}
