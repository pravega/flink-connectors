package io.pravega.connectors.flink; /**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.BatchTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA;

public class FlinkPravegaTableSinkFactory extends FlinkPravegaTableSourceFactoryBase implements StreamTableSinkFactory<Row>, BatchTableSinkFactory<Row> {

    @Override
    public BatchTableSink<Row> createBatchTableSink(Map<String, String> properties) {
        return createFlinkPravegaTableSink(properties);
    }

    @Override
    public StreamTableSink<Row> createStreamTableSink(Map<String, String> properties) {
        return createFlinkPravegaTableSink(properties);
    }

    @Override
    public Map<String, String> requiredContext() {
        return getRequiredContext();
    }

    @Override
    public List<String> supportedProperties() {
        return getSupportedProperties();
    }

    private FlinkPravegaTableSink createFlinkPravegaTableSink(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        final TableSchema schema = descriptorProperties.getTableSchema(SCHEMA());
        SerializationSchema<Row> serializationSchema = getSerializationSchema(properties);

        ConnectorConfigurations connectorConfigurations = new ConnectorConfigurations();
        connectorConfigurations.parseConfigurations(descriptorProperties, ConnectorConfigurations.ConfigurationType.WRITER);

        Pravega.TableSourceWriterBuilder tableSourceWriterBuilder = new Pravega().tableSourceWriterBuilder();
        AbstractWriterBuilder abstractWriterBuilder = tableSourceWriterBuilder;

        if (connectorConfigurations.getTxnLeaseRenewalInterval().isPresent()) {
            tableSourceWriterBuilder.withTxnLeaseRenewalPeriod(Time.milliseconds(connectorConfigurations.getTxnLeaseRenewalInterval().get().longValue()));
        }
        if (connectorConfigurations.getWriterMode().isPresent()) {
            tableSourceWriterBuilder.withWriterMode(connectorConfigurations.getWriterMode().get());
        }
        if (connectorConfigurations.getMetrics().isPresent()) {
            tableSourceWriterBuilder.enableMetrics(connectorConfigurations.getMetrics().get());
        }
        tableSourceWriterBuilder.withRoutingKeyField(connectorConfigurations.getRoutingKey());
        tableSourceWriterBuilder.withSerializationSchema(serializationSchema);
        abstractWriterBuilder = abstractWriterBuilder.forStream(connectorConfigurations.getWriterStream());
        abstractWriterBuilder.withPravegaConfig(connectorConfigurations.getPravegaConfig());

        return new FlinkPravegaTableSinkImpl(tableSourceWriterBuilder::createSinkFunction, tableSourceWriterBuilder::createOutputFormat);
    }

    /**
     * Pravega table sink implementation.
     */
    public static class FlinkPravegaTableSinkImpl extends FlinkPravegaTableSink {

        protected FlinkPravegaTableSinkImpl(Function<TableSinkConfiguration, FlinkPravegaWriter<Row>> writerFactory,
                                            Function<TableSinkConfiguration, FlinkPravegaOutputFormat<Row>> outputFormatFactory) {
            super(writerFactory, outputFormatFactory);
        }

        @Override
        protected FlinkPravegaTableSink createCopy() {
            return new FlinkPravegaTableSinkImpl(writerFactory, outputFormatFactory);
        }
    }
}
