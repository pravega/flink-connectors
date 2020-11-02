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

import io.pravega.client.stream.EventStreamWriter;
import io.pravega.connectors.flink.utils.SchemaRegistryUtils;
import io.pravega.connectors.flink.utils.SetupUtils;
import io.pravega.connectors.flink.utils.SuccessException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public class FlinkPravegaSchemaRegistryReaderTestITCase {

    // Setup utility.
    protected static final SetupUtils SETUP_UTILS = new SetupUtils();

    protected static final SchemaRegistryUtils SCHEMA_REGISTRY_UTILS =
            new SchemaRegistryUtils(SETUP_UTILS, SchemaRegistryUtils.DEFAULT_PORT);

    private static final Schema schema = SchemaBuilder
            .record("MyTest")
            .fields()
            .name("a")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .endRecord();

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

    /**
     * Ignore the test now as failure, waiting for schema registry to refactor the pravega usage to pass
     */
    @Ignore
    @Test
    public void testReaderWithRegistryDeserializer() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        prepareStream(streamName);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkPravegaReader<GenericRecord> reader = FlinkPravegaReader.<GenericRecord>builder()
                .forStream(streamName)
                .enableMetrics(false)
                .withPravegaConfig(SETUP_UTILS.getPravegaConfig().withSchemaRegistryURI(SCHEMA_REGISTRY_UTILS.getSchemaRegistryUri()))
                .withDeserializationSchemafromRegistry(streamName, GenericRecord.class)
                .build();

        env.addSource(reader).addSink(new SinkFunction<GenericRecord>() {
            @Override
            public void invoke(GenericRecord value, Context context) throws Exception {
                if (true) {
                    System.out.println(value.get("a"));
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

    /**
     * Ignore the test now as failure, waiting for schema registry to refactor the pravega usage to pass
     */
    @Ignore
    @Test
    public void testInputFormatWithRegistryDeserializer() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        prepareStream(streamName);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        FlinkPravegaInputFormat<GenericRecord> reader = FlinkPravegaInputFormat.<GenericRecord>builder()
                .forStream(streamName)
                .enableMetrics(false)
                .withPravegaConfig(SETUP_UTILS.getPravegaConfig().withSchemaRegistryURI(SCHEMA_REGISTRY_UTILS.getSchemaRegistryUri()))
                .withDeserializationSchemafromRegistry(streamName, GenericRecord.class)
                .build();

        List<GenericRecord> result = env.createInput(reader, TypeInformation.of(GenericRecord.class)).collect();
        Assert.assertEquals(result.size(), 1);
        Assert.assertEquals(result.get(0), 1);
    }

    // ================================================================================

    private void configurePravegaStream(String streamName, Schema schema) throws Exception {
        SETUP_UTILS.createTestStream(streamName, 1);
        SCHEMA_REGISTRY_UTILS.registerAvroSchema(streamName, schema);
    }

    private void prepareStream(String streamName) throws Exception {
        configurePravegaStream(streamName, schema);
        EventStreamWriter<Object> writer = SCHEMA_REGISTRY_UTILS.getWriter(streamName, schema);
        GenericRecord record = new GenericRecordBuilder(schema).set("a", "test").build();
        writer.writeEvent(record).join();
    }
}
