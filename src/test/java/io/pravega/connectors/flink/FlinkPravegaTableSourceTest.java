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

import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.table.descriptors.Pravega;
import io.pravega.connectors.flink.watermark.LowerBoundAssigner;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.TableDescriptor;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceValidation;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.net.URI;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

/**
 * Unit tests for {@link FlinkPravegaTableSource} and its builder.
 */
public class FlinkPravegaTableSourceTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testTableSourceDescriptor() {
        final String cityName = "fruitName";
        final String total = "count";
        final String eventTime = "eventTime";
        final String procTime = "procTime";
        final String controllerUri = "tcp://localhost:9090";
        final long delay = 3000L;
        final String streamName = "test";
        final String scopeName = "test";

        final TableSchema tableSchema = TableSchema.builder()
                .field(cityName, DataTypes.STRING())
                .field(total, DataTypes.BIGINT())
                .field(eventTime, DataTypes.TIMESTAMP(3))
                .field(procTime, DataTypes.TIMESTAMP(3))
                .build();

        Stream stream = Stream.of(scopeName, streamName);
        PravegaConfig pravegaConfig = PravegaConfig.fromDefaults()
                .withControllerURI(URI.create(controllerUri))
                .withDefaultScope(scopeName);

        // construct table source using descriptors and table source factory
        Pravega pravega = new Pravega();
        pravega.tableSourceReaderBuilder()
                .withReaderGroupScope(stream.getScope())
                .forStream(stream)
                .withPravegaConfig(pravegaConfig);

        final TestTableDescriptor testDesc = new TestTableDescriptor(pravega)
                .withFormat(new Json().failOnMissingField(false))
                .withSchema(
                        new Schema()
                                .field(cityName, DataTypes.STRING())
                                .field(total, DataTypes.BIGINT())
                                .field(eventTime, DataTypes.TIMESTAMP(3))
                                    .rowtime(new Rowtime()
                                                .timestampsFromField(eventTime)
                                                .watermarksFromStrategy(new BoundedOutOfOrderTimestamps(delay))
                                            )
                                .field(procTime, DataTypes.TIMESTAMP(3)).proctime())
                .inAppendMode();

        final Map<String, String> propertiesMap = testDesc.toProperties();
        final TableSource<?> actualSource = TableFactoryService.find(StreamTableSourceFactory.class, propertiesMap)
                .createStreamTableSource(propertiesMap);
        assertNotNull(actualSource);
        TableSourceValidation.validateTableSource(actualSource, tableSchema);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testTableSourceDescriptorWithWatermark() {
        final String cityName = "fruitName";
        final String total = "count";
        final String eventTime = "eventTime";
        final String controllerUri = "tcp://localhost:9090";
        final String streamName = "test";
        final String scopeName = "test";

        Stream stream = Stream.of(scopeName, streamName);
        PravegaConfig pravegaConfig = PravegaConfig.fromDefaults()
                .withControllerURI(URI.create(controllerUri))
                .withDefaultScope(scopeName);

        // construct table source using descriptors and table source factory
        Pravega pravega = new Pravega();
        pravega.tableSourceReaderBuilder()
                .withTimestampAssigner(new MyAssigner())
                .withReaderGroupScope(stream.getScope())
                .forStream(stream)
                .withPravegaConfig(pravegaConfig);

        final TableSchema tableSchema = TableSchema.builder()
                .field(cityName, DataTypes.STRING())
                .field(total, DataTypes.INT())
                .field(eventTime, DataTypes.TIMESTAMP(3))
                .build();

        final TestTableDescriptor testDesc = new TestTableDescriptor(pravega)
                .withFormat(new Json().failOnMissingField(false))
                .withSchema(
                        new Schema()
                                .field(cityName, DataTypes.STRING())
                                .field(total, DataTypes.INT())
                                .field(eventTime, DataTypes.TIMESTAMP(3))
                                .rowtime(new Rowtime()
                                        .timestampsFromSource()
                                        .watermarksFromSource()
                                ))
                .inAppendMode();

        final Map<String, String> propertiesMap = testDesc.toProperties();
        final TableSource<?> actualSource = TableFactoryService.find(StreamTableSourceFactory.class, propertiesMap)
                .createStreamTableSource(propertiesMap);
        assertNotNull(actualSource);
        TableSourceValidation.validateTableSource(actualSource, tableSchema);
    }

    /**
     * Test Table descriptor wrapper
     */
    static class TestTableDescriptor extends TableDescriptor {

        private Schema schemaDescriptor;

        public TestTableDescriptor(ConnectorDescriptor connectorDescriptor) {
            super(connectorDescriptor);
        }

        @Override
        public TestTableDescriptor withFormat(FormatDescriptor format) {
            super.withFormat(format);
            return this;
        }

        public TestTableDescriptor withSchema(Schema schema) {
            this.schemaDescriptor = schema;
            return this;
        }

        @Override
        public TestTableDescriptor inAppendMode() {
            super.inAppendMode();
            return this;
        }

        @Override
        public TestTableDescriptor inRetractMode() {
            super.inRetractMode();
            return this;
        }

        @Override
        public TestTableDescriptor inUpsertMode() {
            super.inUpsertMode();
            return this;
        }

        @Override
        public Map<String, String> additionalProperties() {
            final DescriptorProperties properties = new DescriptorProperties();
            if (schemaDescriptor != null) {
                properties.putProperties(schemaDescriptor.toProperties());
            }
            return properties.asMap();
        }
    }

    public static class MyAssigner extends LowerBoundAssigner<Row> {
        public MyAssigner() {}

        @Override
        public long extractTimestamp(Row element, long previousElementTimestamp) {
            return (long) element.getField(2);
        }

    }
}