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

import io.pravega.client.stream.EventStreamWriter;
import io.pravega.connectors.flink.serialization.DeserializerFromSchemaRegistry;
import io.pravega.connectors.flink.serialization.PravegaDeserializationSchema;
import io.pravega.connectors.flink.utils.SchemaRegistryUtils;
import io.pravega.connectors.flink.utils.SetupUtils;
import io.pravega.connectors.flink.utils.SuccessException;
import io.pravega.connectors.flink.utils.User;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.json.schemas.JSONSchema;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class FlinkPravegaSchemaRegistryReaderTestITCase {

    private static class MyTest {
        public String a;

        public MyTest() {}

        public MyTest(String a) {
            this.a = a;
        }
    }

    // Setup utility.
    protected static final SetupUtils SETUP_UTILS = new SetupUtils();

    protected static final SchemaRegistryUtils SCHEMA_REGISTRY_UTILS =
            new SchemaRegistryUtils(SETUP_UTILS, SchemaRegistryUtils.DEFAULT_PORT);

    private static final Schema SCHEMA = User.SCHEMA$;
    private static final GenericRecord AVRO_EVENT = new GenericRecordBuilder(SCHEMA).set("name", "test").build();
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
    public void testReaderWithAvroGenericRecordRegistryDeserializer() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        prepareAvroStream(streamName, AvroSchema.of(SCHEMA));

        final PravegaConfig pravegaConfig = SETUP_UTILS.getPravegaConfig().withSchemaRegistryURI(SCHEMA_REGISTRY_UTILS.getSchemaRegistryUri());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkPravegaReader<GenericRecord> reader = FlinkPravegaReader.<GenericRecord>builder()
                .forStream(streamName)
                .enableMetrics(false)
                .withPravegaConfig(pravegaConfig)
                .withDeserializationSchema(new PravegaDeserializationSchema<>(
                        new GenericRecordAvroTypeInfo(SCHEMA),
                        new DeserializerFromSchemaRegistry<>(pravegaConfig, streamName, GenericRecord.class)))
                .build();

        env.addSource(reader).addSink(new SinkFunction<GenericRecord>() {
            @Override
            public void invoke(GenericRecord value, Context context) throws Exception {
                if (true) {
                    System.out.println(value.get("name"));
                    throw new SuccessException();
                }
            }
        });

        try {
            env.execute("Schema Registry Read");
        } catch (Exception e) {
            if (!(ExceptionUtils.getRootCause(e) instanceof SuccessException)) {
                throw e;
            }
        }
    }

    @Test
    public void testReaderWithAvroSpecificRecordBaseRegistryDeserializer() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        prepareAvroStream(streamName, AvroSchema.of(SCHEMA));

        final PravegaConfig pravegaConfig = SETUP_UTILS.getPravegaConfig().withSchemaRegistryURI(SCHEMA_REGISTRY_UTILS.getSchemaRegistryUri());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkPravegaReader<User> reader = FlinkPravegaReader.<User>builder()
                .forStream(streamName)
                .enableMetrics(false)
                .withPravegaConfig(pravegaConfig)
                .withDeserializationSchema(new PravegaDeserializationSchema<>(User.class,
                        new DeserializerFromSchemaRegistry<>(pravegaConfig, streamName, User.class)))
                .build();

        env.addSource(reader).addSink(new SinkFunction<User>() {
            @Override
            public void invoke(User value, Context context) throws Exception {
                if (true) {
                    System.out.println(value.get("name"));
                    throw new SuccessException();
                }
            }
        });

        try {
            env.execute("Schema Registry Read");
        } catch (Exception e) {
            if (!(ExceptionUtils.getRootCause(e) instanceof SuccessException)) {
                throw e;
            }
        }
    }

    @Test
    public void testReaderWithJsonRegistryDeserializer() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        prepareJsonStream(streamName, JSONSchema.of(MyTest.class));

        final PravegaConfig pravegaConfig = SETUP_UTILS.getPravegaConfig().withSchemaRegistryURI(SCHEMA_REGISTRY_UTILS.getSchemaRegistryUri());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkPravegaReader<MyTest> reader = FlinkPravegaReader.<MyTest>builder()
                .forStream(streamName)
                .enableMetrics(false)
                .withPravegaConfig(pravegaConfig)
                .withDeserializationSchema(new PravegaDeserializationSchema<>(MyTest.class,
                        new DeserializerFromSchemaRegistry<>(pravegaConfig, streamName, MyTest.class)))
                .build();

        env.addSource(reader).addSink(new SinkFunction<MyTest>() {
            @Override
            public void invoke(MyTest value, Context context) throws Exception {
                if (true) {
                    System.out.println(value.a);
                    throw new SuccessException();
                }
            }
        });

        try {
            env.execute("Schema Registry Read");
        } catch (Exception e) {
            if (!(ExceptionUtils.getRootCause(e) instanceof SuccessException)) {
                throw e;
            }
        }
    }

    @Test
    public void testInputFormatWithAvroRegistryDeserializer() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        prepareAvroStream(streamName, AvroSchema.of(MyTest.class));

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        FlinkPravegaInputFormat<GenericRecord> reader = FlinkPravegaInputFormat.<GenericRecord>builder()
                .forStream(streamName)
                .enableMetrics(false)
                .withPravegaConfig(SETUP_UTILS.getPravegaConfig().withSchemaRegistryURI(SCHEMA_REGISTRY_UTILS.getSchemaRegistryUri()))
                .withDeserializationSchemaFromRegistry(streamName, GenericRecord.class)
                .build();

        List<GenericRecord> result = env.createInput(reader, new GenericRecordAvroTypeInfo(SCHEMA)).collect();
        Assert.assertEquals(result.size(), 1);
        Assert.assertEquals(result.get(0), AVRO_EVENT);
    }

    // ================================================================================

    private void configureAvroPravegaStream(String streamName, AvroSchema schema) throws Exception {
        SETUP_UTILS.createTestStream(streamName, 1);
        SCHEMA_REGISTRY_UTILS.registerSchema(streamName, schema, SerializationFormat.Avro);
    }

    private void prepareAvroStream(String streamName, AvroSchema schema) throws Exception {
        configureAvroPravegaStream(streamName, schema);
        EventStreamWriter<Object> writer = SCHEMA_REGISTRY_UTILS.getWriter(streamName, schema, SerializationFormat.Avro);
        writer.writeEvent(AVRO_EVENT).join();
    }

    private void configureJsonPravegaStream(String streamName, JSONSchema schema) throws Exception {
        SETUP_UTILS.createTestStream(streamName, 1);
        SCHEMA_REGISTRY_UTILS.registerSchema(streamName, schema, SerializationFormat.Json);
    }

    private void prepareJsonStream(String streamName, JSONSchema schema) throws Exception {
        configureJsonPravegaStream(streamName, schema);
        EventStreamWriter<Object> writer = SCHEMA_REGISTRY_UTILS.getWriter(streamName, schema, SerializationFormat.Json);
        writer.writeEvent(JSON_EVENT).join();
    }
}
