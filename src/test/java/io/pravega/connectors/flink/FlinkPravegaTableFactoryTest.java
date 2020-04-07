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

import io.pravega.connectors.flink.FlinkPravegaTableSourceTest.TestTableDescriptor;
import io.pravega.client.stream.Stream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.NoMatchingTableFactoryException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceValidation;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.junit.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static io.pravega.connectors.flink.Pravega.CONNECTOR_WRITER_MODE;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Unit test that validates configurations that can be passed to create source and sink factory.
 */
public class FlinkPravegaTableFactoryTest {

    final static Schema SCHEMA = new Schema()
                                            .field("name", DataTypes.STRING())
                                            .field("age", DataTypes.INT());
    final static Json JSON = new Json().failOnMissingField(false);
    final static String SCOPE = "foo";
    final static String STREAM = "bar";
    final static String CONTROLLER_URI = "tcp://localhost:9090";

    final static PravegaConfig PRAVEGA_CONFIG = PravegaConfig.fromDefaults()
            .withControllerURI(URI.create(CONTROLLER_URI))
            .withDefaultScope(SCOPE);

    /**
     * Processing time attribute should be of type TIMESTAMP.
     */
    @Test (expected = ValidationException.class)
    public void testWrongProcTimeAttributeType() {
        final Schema schema = new Schema()
                .field("name", DataTypes.STRING())
                .field("age", DataTypes.INT()).proctime();

        Pravega pravega = new Pravega();
        Stream stream = Stream.of(SCOPE, STREAM);
        pravega.tableSourceReaderBuilder()
                .forStream(stream)
                .withPravegaConfig(PRAVEGA_CONFIG);
        final TestTableDescriptor testDesc = new TestTableDescriptor(pravega)
                .withFormat(JSON)
                .withSchema(schema)
                .inAppendMode();
        final Map<String, String> propertiesMap = testDesc.toProperties();
        FlinkPravegaTableFactoryBase tableFactoryBase = new FlinkPravegaStreamTableSourceFactory();
        tableFactoryBase.createFlinkPravegaTableSource(propertiesMap);
        fail("Schema validation failed");
    }

    /**
     * Rowtime attribute should be of type TIMESTAMP.
     */
    @Test (expected = ValidationException.class)
    public void testWrongRowTimeAttributeType() {
        final Schema schema = new Schema()
                .field("name", DataTypes.STRING())
                .field("age", DataTypes.INT()).rowtime(new Rowtime()
                                                                .timestampsFromField("age")
                                                                .watermarksFromStrategy(
                                                                        new BoundedOutOfOrderTimestamps(30000L)));
        Pravega pravega = new Pravega();
        Stream stream = Stream.of(SCOPE, STREAM);
        pravega.tableSourceReaderBuilder()
                .forStream(stream)
                .withPravegaConfig(PRAVEGA_CONFIG);
        final TestTableDescriptor testDesc = new TestTableDescriptor(pravega)
                .withFormat(JSON)
                .withSchema(schema)
                .inAppendMode();
        final Map<String, String> propertiesMap = testDesc.toProperties();
        FlinkPravegaTableFactoryBase tableFactoryBase = new FlinkPravegaStreamTableSourceFactory();
        tableFactoryBase.createFlinkPravegaTableSource(propertiesMap);
        fail("Schema validation failed");
    }


    /**
     * Scope should be supplied either through {@link PravegaConfig} or {@link Pravega.TableSourceReaderBuilder}.
     */
    @Test (expected = IllegalStateException.class)
    public void testMissingRGScopeFail() {

        Pravega pravega = new Pravega();
        Stream stream = Stream.of(SCOPE, STREAM);

        pravega.tableSourceReaderBuilder()
                .forStream(stream);

        final TestTableDescriptor testDesc = new TestTableDescriptor(pravega)
                .withFormat(JSON)
                .withSchema(SCHEMA)
                .inAppendMode();

        final Map<String, String> propertiesMap = testDesc.toProperties();

        FlinkPravegaTableFactoryBase tableFactoryBase = new FlinkPravegaStreamTableSourceFactory();
        tableFactoryBase.createFlinkPravegaTableSource(propertiesMap);
        fail("scope validation failed");
    }

    /**
     * Should use the supplied Pravega configurations to derive the reader group scope.
     */
    @Test
    public void testMissingRGScope() {

        Pravega pravega = new Pravega();
        Stream stream = Stream.of(SCOPE, STREAM);

        pravega.tableSourceReaderBuilder()
                .forStream(stream)
                .withPravegaConfig(PRAVEGA_CONFIG);

        final TestTableDescriptor testDesc = new TestTableDescriptor(pravega)
                .withFormat(JSON)
                .withSchema(SCHEMA)
                .inAppendMode();

        final Map<String, String> propertiesMap = testDesc.toProperties();

        FlinkPravegaTableFactoryBase tableFactoryBase = new FlinkPravegaStreamTableSourceFactory();
        tableFactoryBase.createFlinkPravegaTableSource(propertiesMap);
    }

    /**
     * Stream table source expects 'update-mode' configuration to be passed.
     */
    @Test (expected = NoMatchingTableFactoryException.class)
    public void testMissingStreamMode() {

        Pravega pravega = new Pravega();
        Stream stream = Stream.of(SCOPE, STREAM);

        pravega.tableSourceReaderBuilder()
                .forStream(stream)
                .withPravegaConfig(PRAVEGA_CONFIG);

        final TestTableDescriptor testDesc = new TestTableDescriptor(pravega)
                .withFormat(JSON)
                .withSchema(SCHEMA);

        final Map<String, String> propertiesMap = testDesc.toProperties();

        final TableSource<?> source = TableFactoryService.find(StreamTableSourceFactory.class, propertiesMap)
                .createStreamTableSource(propertiesMap);
        TableSourceValidation.validateTableSource(source, TableSchema.builder()
                .field("name", DataTypes.STRING() )
                .field("age", DataTypes.INT())
                .build());
        fail("update mode configuration validation failed");
    }

    /**
     * For sink, stream name information is mandatory.
     */
    @Test (expected = IllegalStateException.class)
    public void testMissingStreamNameForWriter() {
        Pravega pravega = new Pravega();

        pravega.tableSinkWriterBuilder()
                .withRoutingKeyField("name");

        final TestTableDescriptor testDesc = new TestTableDescriptor(pravega)
                .withFormat(JSON)
                .withSchema(SCHEMA)
                .inAppendMode();

        final Map<String, String> propertiesMap = testDesc.toProperties();
        TableFactoryService.find(StreamTableSinkFactory.class, propertiesMap)
                .createStreamTableSink(propertiesMap);
        fail("stream name validation failed");
    }

    /**
     * For sink, routingkey-field-name information is mandatory.
     */
    @Test (expected = ValidationException.class)
    public void testMissingRoutingKeyForWriter() {
        Pravega pravega = new Pravega();
        Stream stream = Stream.of(SCOPE, STREAM);

        pravega.tableSinkWriterBuilder()
                .forStream(stream)
                .withPravegaConfig(PRAVEGA_CONFIG);

        final TestTableDescriptor testDesc = new TestTableDescriptor(pravega)
                .withFormat(JSON)
                .withSchema(SCHEMA)
                .inAppendMode();

        final Map<String, String> propertiesMap = testDesc.toProperties();
        TableFactoryService.find(StreamTableSinkFactory.class, propertiesMap)
                .createStreamTableSink(propertiesMap);
        fail("routingKey field name validation failed");
    }

    @Test (expected = ValidationException.class)
    public void testInvalidWriterMode() {
        Pravega pravega = new Pravega();
        Stream stream = Stream.of(SCOPE, STREAM);

        pravega.tableSinkWriterBuilder()
                .withRoutingKeyField("name")
                .forStream(stream)
                .withPravegaConfig(PRAVEGA_CONFIG);

        final TestTableDescriptor testDesc = new TestTableDescriptor(pravega)
                .withFormat(JSON)
                .withSchema(SCHEMA)
                .inAppendMode();

        final Map<String, String> propertiesMap = testDesc.toProperties();
        Map<String, String> test = new HashMap<>(propertiesMap);
        test.put(CONNECTOR_WRITER_MODE, "foo");
        TableFactoryService.find(StreamTableSinkFactory.class, test)
                .createStreamTableSink(test);
        fail("writer mode validation failed");
    }

    @Test
    public void testValidWriterModeAtleastOnce() {
        Pravega pravega = new Pravega();
        Stream stream = Stream.of(SCOPE, STREAM);

        pravega.tableSinkWriterBuilder()
                .withRoutingKeyField("name").withWriterMode(PravegaWriterMode.ATLEAST_ONCE)
                .forStream(stream)
                .withPravegaConfig(PRAVEGA_CONFIG);

        final TestTableDescriptor testDesc = new TestTableDescriptor(pravega)
                .withFormat(JSON)
                .withSchema(SCHEMA)
                .inAppendMode();

        final Map<String, String> propertiesMap = testDesc.toProperties();
        final TableSink<?> sink = TableFactoryService.find(StreamTableSinkFactory.class, propertiesMap)
                .createStreamTableSink(propertiesMap);
        assertNotNull(sink);
    }

    @Test
    public void testValidWriterModeExactlyOnce() {
        Pravega pravega = new Pravega();
        Stream stream = Stream.of(SCOPE, STREAM);

        pravega.tableSinkWriterBuilder()
                .withRoutingKeyField("name").withWriterMode(PravegaWriterMode.EXACTLY_ONCE)
                .forStream(stream)
                .withPravegaConfig(PRAVEGA_CONFIG);

        final TestTableDescriptor testDesc = new TestTableDescriptor(pravega)
                .withFormat(JSON)
                .withSchema(SCHEMA)
                .inAppendMode();

        final Map<String, String> propertiesMap = testDesc.toProperties();
        final TableSink<?> sink = TableFactoryService.find(StreamTableSinkFactory.class, propertiesMap)
                .createStreamTableSink(propertiesMap);
        assertNotNull(sink);
    }

    @Test (expected = ValidationException.class)
    public void testMissingFormatDefinition() {
        Pravega pravega = new Pravega();
        Stream stream = Stream.of(SCOPE, STREAM);

        pravega.tableSinkWriterBuilder()
                .withRoutingKeyField("name")
                .forStream(stream)
                .withPravegaConfig(PRAVEGA_CONFIG);

        final TestTableDescriptor testDesc = new TestTableDescriptor(pravega)
                .withSchema(SCHEMA)
                .inAppendMode();

        final Map<String, String> propertiesMap = testDesc.toProperties();
        TableFactoryService.find(StreamTableSinkFactory.class, propertiesMap)
                .createStreamTableSink(propertiesMap);
        fail("table factory validation failed");
    }

    @Test (expected = ValidationException.class)
    public void testMissingSchemaDefinition() {
        Pravega pravega = new Pravega();
        Stream stream = Stream.of(SCOPE, STREAM);

        pravega.tableSinkWriterBuilder()
                .withRoutingKeyField("name")
                .forStream(stream)
                .withPravegaConfig(PRAVEGA_CONFIG);

        final TestTableDescriptor testDesc = new TestTableDescriptor(pravega)
                .withFormat(JSON)
                .inAppendMode();

        final Map<String, String> propertiesMap = testDesc.toProperties();
        TableFactoryService.find(StreamTableSinkFactory.class, propertiesMap)
                .createStreamTableSink(propertiesMap);
        fail("missing schema validation failed");
    }

}
