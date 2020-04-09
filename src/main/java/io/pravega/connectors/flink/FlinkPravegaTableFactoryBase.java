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

import io.pravega.client.stream.StreamCut;
import io.pravega.connectors.flink.util.ConnectorConfigurations;
import io.pravega.connectors.flink.util.StreamWithBoundaries;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.pravega.connectors.flink.Pravega.*;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_FROM;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_TYPE;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_DELAY;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_FROM;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_PROCTIME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_DATA_TYPE;

public abstract class FlinkPravegaTableFactoryBase {

    protected Map<String, String> getRequiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_PRAVEGA);

        // to preserve backward compatibility in case the connector property format changes
        // It is not clear on how the framework is making use of the "connector.property-version" but all the implementation classes supplies value as "1"
        // https://github.com/apache/flink/blob/release-1.6.0/flink-libraries/flink-table/src/main/scala/org/apache/flink/table/descriptors/ConnectorDescriptorValidator.scala#L45
        // https://github.com/apache/flink/blob/release-1.6.0/flink-libraries/flink-table/src/main/scala/org/apache/flink/table/factories/TableFactoryService.scala#L202
        // https://github.com/apache/flink/blob/release-1.6.0/flink-connectors/flink-connector-kafka-base/src/main/java/org/apache/flink/streaming/connectors/kafka/KafkaTableSourceSinkFactoryBase.java#L98
        context.put(CONNECTOR_PROPERTY_VERSION, "1");

        context.put(CONNECTOR_VERSION, getVersion());
        return context;
    }

    protected List<String> getSupportedProperties() {
        List<String> properties = new ArrayList<>();

        // Pravega
        properties.add(CONNECTOR_METRICS);

        properties.add(CONNECTOR_CONNECTION_CONFIG);
        properties.add(CONNECTOR_CONNECTION_CONFIG_CONTROLLER_URI);
        properties.add(CONNECTOR_CONNECTION_CONFIG_DEFAULT_SCOPE);
        properties.add(CONNECTOR_CONNECTION_CONFIG_SECURITY);
        properties.add(CONNECTOR_CONNECTION_CONFIG_SECURITY_AUTH_TYPE);
        properties.add(CONNECTOR_CONNECTION_CONFIG_SECURITY_AUTH_TOKEN);
        properties.add(CONNECTOR_CONNECTION_CONFIG_SECURITY_VALIDATE_HOSTNAME);
        properties.add(CONNECTOR_CONNECTION_CONFIG_SECURITY_TRUST_STORE);

        properties.add(CONNECTOR_READER);
        properties.add(CONNECTOR_READER_STREAM_INFO);
        properties.add(CONNECTOR_READER_STREAM_INFO + ".#." + CONNECTOR_READER_STREAM_INFO_SCOPE);
        properties.add(CONNECTOR_READER_STREAM_INFO + ".#." + CONNECTOR_READER_STREAM_INFO_STREAM);
        properties.add(CONNECTOR_READER_STREAM_INFO + ".#." + CONNECTOR_READER_STREAM_INFO_START_STREAMCUT);
        properties.add(CONNECTOR_READER_STREAM_INFO + ".#." + CONNECTOR_READER_STREAM_INFO_END_STREAMCUT);
        properties.add(CONNECTOR_READER_READER_GROUP);
        properties.add(CONNECTOR_READER_READER_GROUP_UID);
        properties.add(CONNECTOR_READER_READER_GROUP_SCOPE);
        properties.add(CONNECTOR_READER_READER_GROUP_NAME);
        properties.add(CONNECTOR_READER_READER_GROUP_REFRESH_INTERVAL);
        properties.add(CONNECTOR_READER_READER_GROUP_EVENT_READ_TIMEOUT_INTERVAL);
        properties.add(CONNECTOR_READER_READER_GROUP_CHECKPOINT_INITIATE_TIMEOUT_INTERVAL);
        properties.add(CONNECTOR_READER_USER_TIMESTAMP_ASSIGNER);

        properties.add(CONNECTOR_WRITER);
        properties.add(CONNECTOR_WRITER_SCOPE);
        properties.add(CONNECTOR_WRITER_STREAM);
        properties.add(CONNECTOR_WRITER_MODE);
        properties.add(CONNECTOR_WRITER_TXN_LEASE_RENEWAL_INTERVAL);
        properties.add(CONNECTOR_WRITER_ENABLE_WATERMARK);
        properties.add(CONNECTOR_WRITER_ROUTING_KEY_FILED_NAME);

        // schema
        properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_NAME);
        properties.add(SCHEMA + ".#." + SCHEMA_FROM);

        // time attributes
        properties.add(SCHEMA + ".#." + SCHEMA_PROCTIME);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_TYPE);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_FROM);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_CLASS);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_SERIALIZED);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_TYPE);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_CLASS);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_SERIALIZED);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_DELAY);

        // format wildcard
        properties.add(FORMAT + ".*");

        return properties;
    }

    protected abstract String getVersion();

    protected abstract boolean isStreamEnvironment();

    protected DescriptorProperties getValidatedProperties(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);

        boolean supportsSourceTimestamps = true;
        boolean supportsSourceWatermarks = true;
        new SchemaValidator(isStreamEnvironment(), supportsSourceTimestamps, supportsSourceWatermarks).validate(descriptorProperties);

        new PravegaValidator().validate(descriptorProperties);

        return descriptorProperties;
    }

    protected SerializationSchema<Row> getSerializationSchema(Map<String, String> properties) {
        @SuppressWarnings("unchecked")
        final SerializationSchemaFactory<Row> formatFactory = TableFactoryService.find(
                SerializationSchemaFactory.class,
                properties,
                this.getClass().getClassLoader());
        return formatFactory.createSerializationSchema(properties);
    }

    protected DeserializationSchema<Row> getDeserializationSchema(Map<String, String> properties) {
        @SuppressWarnings("unchecked")
        final DeserializationSchemaFactory<Row> formatFactory = TableFactoryService.find(
                DeserializationSchemaFactory.class,
                properties,
                this.getClass().getClassLoader());
        return formatFactory.createDeserializationSchema(properties);
    }

    @SuppressWarnings("unchecked")
    protected FlinkPravegaTableSource createFlinkPravegaTableSource(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        final TableSchema schema = descriptorProperties.getTableSchema(SCHEMA);
        final DeserializationSchema<Row> deserializationSchema = getDeserializationSchema(properties);

        ConnectorConfigurations connectorConfigurations = new ConnectorConfigurations();
        connectorConfigurations.parseConfigurations(descriptorProperties, ConnectorConfigurations.ConfigurationType.READER);

        // create source from the reader builder by using the supplied properties
        TableSourceReaderBuilder tableSourceReaderBuilder = new Pravega().tableSourceReaderBuilder();
        tableSourceReaderBuilder.withDeserializationSchema(deserializationSchema);

        if (connectorConfigurations.getAssignerWithTimeWindows().isPresent()) {
            tableSourceReaderBuilder.withTimestampAssigner(connectorConfigurations.getAssignerWithTimeWindows().get());
        }

        if (connectorConfigurations.getUid().isPresent()) {
            tableSourceReaderBuilder.uid(connectorConfigurations.getUid().get());
        }
        if (connectorConfigurations.getRgScope().isPresent()) {
            tableSourceReaderBuilder.withReaderGroupScope(connectorConfigurations.getRgScope().get());
        }
        if (connectorConfigurations.getRgName().isPresent()) {
            tableSourceReaderBuilder.withReaderGroupName(connectorConfigurations.getRgName().get());
        }
        if (connectorConfigurations.getRefreshInterval().isPresent()) {
            tableSourceReaderBuilder.withReaderGroupRefreshTime(Time.milliseconds(connectorConfigurations.getRefreshInterval().get()));
        }
        if (connectorConfigurations.getEventReadTimeoutInterval().isPresent()) {
            tableSourceReaderBuilder.withEventReadTimeout(Time.milliseconds(connectorConfigurations.getEventReadTimeoutInterval().get()));
        }
        if (connectorConfigurations.getCheckpointInitiateTimeoutInterval().isPresent()) {
            tableSourceReaderBuilder.withCheckpointInitiateTimeout(Time.milliseconds(connectorConfigurations.getCheckpointInitiateTimeoutInterval().get()));
        }

        tableSourceReaderBuilder.withPravegaConfig(connectorConfigurations.getPravegaConfig());
        if (connectorConfigurations.getMetrics().isPresent()) {
            tableSourceReaderBuilder.enableMetrics(connectorConfigurations.getMetrics().get());
        }
        tableSourceReaderBuilder.withPravegaConfig(connectorConfigurations.getPravegaConfig());
        for (StreamWithBoundaries streamWithBoundaries: connectorConfigurations.getReaderStreams()) {
            if (streamWithBoundaries.getFrom() != StreamCut.UNBOUNDED && streamWithBoundaries.getTo() != StreamCut.UNBOUNDED) {
                tableSourceReaderBuilder.forStream(streamWithBoundaries.getStream(), streamWithBoundaries.getFrom(), streamWithBoundaries.getTo());
            } else if (streamWithBoundaries.getFrom() != StreamCut.UNBOUNDED) {
                tableSourceReaderBuilder.forStream(streamWithBoundaries.getStream(), streamWithBoundaries.getFrom());
            } else {
                tableSourceReaderBuilder.forStream(streamWithBoundaries.getStream());
            }
        }

        FlinkPravegaTableSource flinkPravegaTableSource = new FlinkPravegaTableSourceImpl(
                                                                    tableSourceReaderBuilder::buildSourceFunction,
                                                                    tableSourceReaderBuilder::buildInputFormat, schema,
                                                                    new RowTypeInfo(schema.getFieldTypes(), schema.getFieldNames()));
        flinkPravegaTableSource.setRowtimeAttributeDescriptors(SchemaValidator.deriveRowtimeAttributes(descriptorProperties));
        Optional<String> procTimeAttribute = SchemaValidator.deriveProctimeAttribute(descriptorProperties);
        if (procTimeAttribute.isPresent()) {
            flinkPravegaTableSource.setProctimeAttribute(procTimeAttribute.get());
        }

        return flinkPravegaTableSource;
    }

    @SuppressWarnings("unchecked")
    protected FlinkPravegaTableSink createFlinkPravegaTableSink(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        final TableSchema schema = descriptorProperties.getTableSchema(SCHEMA);
        SerializationSchema<Row> serializationSchema = getSerializationSchema(properties);

        ConnectorConfigurations connectorConfigurations = new ConnectorConfigurations();
        connectorConfigurations.parseConfigurations(descriptorProperties, ConnectorConfigurations.ConfigurationType.WRITER);

        TableSinkWriterBuilder tableSinkWriterBuilder = new Pravega().tableSinkWriterBuilder();
        if (connectorConfigurations.getTxnLeaseRenewalInterval().isPresent()) {
            tableSinkWriterBuilder.withTxnLeaseRenewalPeriod(Time.milliseconds(connectorConfigurations.getTxnLeaseRenewalInterval().get().longValue()));
        }
        if (connectorConfigurations.getWriterMode().isPresent()) {
            tableSinkWriterBuilder.withWriterMode(connectorConfigurations.getWriterMode().get());
        }
        if (connectorConfigurations.getMetrics().isPresent()) {
            tableSinkWriterBuilder.enableMetrics(connectorConfigurations.getMetrics().get());
        }
        tableSinkWriterBuilder.withPravegaConfig(connectorConfigurations.getPravegaConfig());
        tableSinkWriterBuilder.enableWatermark(connectorConfigurations.getWatermark());
        tableSinkWriterBuilder.withRoutingKeyField(connectorConfigurations.getRoutingKey());
        tableSinkWriterBuilder.withSerializationSchema(serializationSchema);
        tableSinkWriterBuilder.forStream(connectorConfigurations.getWriterStream());
        tableSinkWriterBuilder.withPravegaConfig(connectorConfigurations.getPravegaConfig());

        return new FlinkPravegaTableSinkImpl(tableSinkWriterBuilder::createSinkFunction,
                tableSinkWriterBuilder::createOutputFormat)
                .configure(schema.getFieldNames(), schema.getFieldTypes());
    }

    public static final class FlinkPravegaTableSourceImpl extends FlinkPravegaTableSource {

        /**
         * Creates a Pravega Table Source Implementation Instance {@link FlinkPravegaTableSource}.
         *
         * @param sourceFunctionFactory a factory for the {@link FlinkPravegaReader} to implement {@link StreamTableSource}
         * @param inputFormatFactory    a factory for the {@link FlinkPravegaInputFormat} to implement {@link BatchTableSource}
         * @param schema                the table schema
         * @param returnType            the return type based on the table schema
         */
        protected FlinkPravegaTableSourceImpl(Supplier<FlinkPravegaReader<Row>> sourceFunctionFactory,
                                              Supplier<FlinkPravegaInputFormat<Row>> inputFormatFactory,
                                              TableSchema schema, TypeInformation<Row> returnType) {
            super(sourceFunctionFactory, inputFormatFactory, schema, returnType);
        }

        @Override
        public String explainSource() {
            return "FlinkPravegaTableSource";
        }
    }

    /**
     * Pravega table sink implementation.
     */
    public static final class FlinkPravegaTableSinkImpl extends FlinkPravegaTableSink {

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
