/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink;

import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.connectors.flink.util.StreamWithBoundaries;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.factories.BatchTableSourceFactory;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.Optional;

import static io.pravega.connectors.flink.Pravega.CONNECTOR_CONNECTION_CONFIG_SECURITY_AUTH_TYPE;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_CONNECTION_CONFIG_SECURITY_AUTH_TOKEN;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_CONNECTION_CONFIG_SECURITY_VALIDATE_HOSTNAME;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_CONNECTION_CONFIG_SECURITY_TRUST_STORE;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_CONNECTION_CONFIG_CONTROLLER_URI;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_CONNECTION_CONFIG_DEFAULT_SCOPE;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_READER_STREAM_INFO_SCOPE;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_READER_STREAM_INFO_STREAM;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_READER_STREAM_INFO;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_READER_STREAM_INFO_START_STREAMCUT;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_READER_STREAM_INFO_END_STREAMCUT;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_READER_READER_GROUP_UID;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_READER_READER_GROUP_SCOPE;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_READER_READER_GROUP_NAME;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_READER_READER_GROUP_REFRESH_INTERVAL;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_READER_READER_GROUP_EVENT_READ_TIMEOUT_INTERVAL;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_READER_READER_GROUP_CHECKPOINT_INITIATE_TIMEOUT_INTERVAL;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_METRICS;

import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA;

public class FlinkPravegaTableSourceFactory extends FlinkPravegaTableSourceFactoryBase implements StreamTableSourceFactory<Row>, BatchTableSourceFactory<Row> {

    @Override
    public Map<String, String> requiredContext() {
        return getRequiredContext();
    }

    @Override
    public List<String> supportedProperties() {
        return getSupportedProperties();
    }

    @Override
    public BatchTableSource<Row> createBatchTableSource(Map<String, String> properties) {
        return createFlinkPravegaTableSource(properties);
    }

    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
        return createFlinkPravegaTableSource(properties);
    }

    private FlinkPravegaTableSource createFlinkPravegaTableSource(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        final TableSchema schema = descriptorProperties.getTableSchema(SCHEMA());
        final DeserializationSchema<Row> deserializationSchema = getDeserializationSchema(properties);

        ConnectorConfigurations connectorConfigurations = new ConnectorConfigurations();
        connectorConfigurations.parseConfigurations(descriptorProperties, ConnectorConfigurations.ConfigurationType.READER);

        Pravega.TableSourceReaderBuilder tableSourceReaderBuilder = new Pravega().tableSourceReaderBuilder();

        AbstractStreamingReaderBuilder abstractStreamingReaderBuilder = tableSourceReaderBuilder;
        if (connectorConfigurations.getUid().isPresent()) {
            abstractStreamingReaderBuilder = abstractStreamingReaderBuilder.uid(connectorConfigurations.getUid().get());
        }
        if (connectorConfigurations.getRgScope().isPresent()) {
            abstractStreamingReaderBuilder = abstractStreamingReaderBuilder.withReaderGroupScope(connectorConfigurations.getRgScope().get());
        }
        if (connectorConfigurations.getRgName().isPresent()) {
            abstractStreamingReaderBuilder = abstractStreamingReaderBuilder.withReaderGroupName(connectorConfigurations.getRgName().get());
        }
        if (connectorConfigurations.getRefreshInterval().isPresent()) {
            abstractStreamingReaderBuilder = abstractStreamingReaderBuilder.withReaderGroupRefreshTime(Time.milliseconds(connectorConfigurations.getRefreshInterval().get()));
        }
        if (connectorConfigurations.getEventReadTimeoutInterval().isPresent()) {
            abstractStreamingReaderBuilder = abstractStreamingReaderBuilder.withEventReadTimeout(Time.milliseconds(connectorConfigurations.getEventReadTimeoutInterval().get()));
        }
        if (connectorConfigurations.getCheckpointInitiateTimeoutInterval().isPresent()) {
            abstractStreamingReaderBuilder = abstractStreamingReaderBuilder.withCheckpointInitiateTimeout(Time.milliseconds(connectorConfigurations.getCheckpointInitiateTimeoutInterval().get()));
        }

        AbstractReaderBuilder abstractReaderBuilder = abstractStreamingReaderBuilder;
        abstractReaderBuilder = abstractReaderBuilder.withPravegaConfig(connectorConfigurations.getPravegaConfig());

        if (connectorConfigurations.getMetrics().isPresent()) {
            abstractReaderBuilder = abstractReaderBuilder.enableMetrics(connectorConfigurations.getMetrics().get());
        }

        for (StreamWithBoundaries streamWithBoundaries: connectorConfigurations.getReaderStreams()) {
            if (streamWithBoundaries.getFrom() != StreamCut.UNBOUNDED && streamWithBoundaries.getTo() != StreamCut.UNBOUNDED) {
                abstractReaderBuilder = abstractReaderBuilder.forStream(streamWithBoundaries.getStream(), streamWithBoundaries.getFrom(), streamWithBoundaries.getTo());
            } else if (streamWithBoundaries.getFrom() != StreamCut.UNBOUNDED) {
                abstractReaderBuilder = abstractReaderBuilder.forStream(streamWithBoundaries.getStream(), streamWithBoundaries.getFrom());
            } else {
                abstractReaderBuilder = abstractReaderBuilder.forStream(streamWithBoundaries.getStream());
            }
        }

        tableSourceReaderBuilder.withDeserializationSchema(deserializationSchema);

        Supplier<FlinkPravegaReader<Row>> sourceFunctionFactory = abstractStreamingReaderBuilder::buildSourceFunction;
        Supplier<FlinkPravegaInputFormat<Row>> inputFormatFactory = tableSourceReaderBuilder::buildInputFormat;
        RowTypeInfo returnType = new RowTypeInfo(schema.getTypes(), schema.getColumnNames());

        FlinkPravegaTableSourceImpl flinkPravegaTableSourceImpl = new FlinkPravegaTableSourceImpl(sourceFunctionFactory, inputFormatFactory, schema, returnType);
        flinkPravegaTableSourceImpl.setRowtimeAttributeDescriptors(SchemaValidator.deriveRowtimeAttributes(descriptorProperties));
        Optional<String> proctimeAttribute = SchemaValidator.deriveProctimeAttribute(descriptorProperties);
        if (proctimeAttribute.isPresent()) {
            flinkPravegaTableSourceImpl.setProctimeAttribute(proctimeAttribute.get());
        }

        return flinkPravegaTableSourceImpl;
    }

    private FlinkPravegaTableSource createFlinkPravegaTableSourceOld(Map<String, String> properties) {

        final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        final TableSchema schema = descriptorProperties.getTableSchema(SCHEMA());
        final DeserializationSchema<Row> deserializationSchema = getDeserializationSchema(properties);

        final String controllerUri = descriptorProperties.getString(CONNECTOR_CONNECTION_CONFIG_CONTROLLER_URI);
        final Optional<String> defaultScope = descriptorProperties.getOptionalString(CONNECTOR_CONNECTION_CONFIG_DEFAULT_SCOPE);
        final Optional<Boolean> metrics =  descriptorProperties.getOptionalBoolean(CONNECTOR_METRICS);

        final Optional<String> authType = descriptorProperties.getOptionalString(CONNECTOR_CONNECTION_CONFIG_SECURITY_AUTH_TYPE);
        final Optional<String> authToken = descriptorProperties.getOptionalString(CONNECTOR_CONNECTION_CONFIG_SECURITY_AUTH_TOKEN);
        final Optional<Boolean> validateHostName =  descriptorProperties.getOptionalBoolean(CONNECTOR_CONNECTION_CONFIG_SECURITY_VALIDATE_HOSTNAME);
        final Optional<String> trustStore = descriptorProperties.getOptionalString(CONNECTOR_CONNECTION_CONFIG_SECURITY_TRUST_STORE);

        final Optional<String> uid = descriptorProperties.getOptionalString(CONNECTOR_READER_READER_GROUP_UID);
        final Optional<String> rgScope = descriptorProperties.getOptionalString(CONNECTOR_READER_READER_GROUP_SCOPE);
        final Optional<String> rgName = descriptorProperties.getOptionalString(CONNECTOR_READER_READER_GROUP_NAME);
        final Optional<Long> refreshInterval = descriptorProperties.getOptionalLong(CONNECTOR_READER_READER_GROUP_REFRESH_INTERVAL);
        final Optional<Long> eventReadTimeoutInterval = descriptorProperties.getOptionalLong(CONNECTOR_READER_READER_GROUP_EVENT_READ_TIMEOUT_INTERVAL);
        final Optional<Long> checkpointInitiateTimeoutInterval = descriptorProperties.getOptionalLong(CONNECTOR_READER_READER_GROUP_CHECKPOINT_INITIATE_TIMEOUT_INTERVAL);

        if (!defaultScope.isPresent() && !rgScope.isPresent()) {
            throw new ValidationException("Must supply either " + CONNECTOR_READER_READER_GROUP_SCOPE + " or " + CONNECTOR_CONNECTION_CONFIG_DEFAULT_SCOPE);
        }
        final String scope = defaultScope.isPresent() ? defaultScope.get() : rgScope.get();

        PravegaConfig pravegaConfig = PravegaConfig.fromDefaults()
                .withControllerURI(URI.create(controllerUri))
                .withDefaultScope(scope)
                .withCredentials(new DefaultCredentials("1111_aaaa", "admin")); //TODO get auth type and auth token and create a new Security impl instance
        if (validateHostName.isPresent()) {
            pravegaConfig = pravegaConfig.withHostnameValidation(validateHostName.get());
        }
        if (trustStore.isPresent()) {
            pravegaConfig = pravegaConfig.withTrustStore(trustStore.get());
        }

        Pravega.TableSourceReaderBuilder tableSourceReaderBuilder = new Pravega().tableSourceReaderBuilder();

        AbstractStreamingReaderBuilder abstractStreamingReaderBuilder = tableSourceReaderBuilder;
        if (uid.isPresent()) {
            abstractStreamingReaderBuilder = abstractStreamingReaderBuilder.uid(uid.get());
        }
        if (rgScope.isPresent()) {
            abstractStreamingReaderBuilder = abstractStreamingReaderBuilder.withReaderGroupScope(rgScope.get());
        }
        if (rgName.isPresent()) {
            abstractStreamingReaderBuilder = abstractStreamingReaderBuilder.withReaderGroupName(rgName.get());
        }
        if (refreshInterval.isPresent()) {
            abstractStreamingReaderBuilder = abstractStreamingReaderBuilder.withReaderGroupRefreshTime(Time.milliseconds(refreshInterval.get()));
        }
        if (eventReadTimeoutInterval.isPresent()) {
            abstractStreamingReaderBuilder = abstractStreamingReaderBuilder.withEventReadTimeout(Time.milliseconds(eventReadTimeoutInterval.get()));
        }
        if (checkpointInitiateTimeoutInterval.isPresent()) {
            abstractStreamingReaderBuilder = abstractStreamingReaderBuilder.withCheckpointInitiateTimeout(Time.milliseconds(checkpointInitiateTimeoutInterval.get()));
        }

        AbstractReaderBuilder abstractReaderBuilder = abstractStreamingReaderBuilder;
        if (metrics.isPresent()) {
            abstractReaderBuilder = abstractReaderBuilder.enableMetrics(metrics.get());
        }

        final List<Map<String, String>> streamPropsList = descriptorProperties.getFixedIndexedProperties(
                CONNECTOR_READER_STREAM_INFO,
                Arrays.asList(CONNECTOR_READER_STREAM_INFO_SCOPE,
                        CONNECTOR_READER_STREAM_INFO_STREAM,
                        CONNECTOR_READER_STREAM_INFO_START_STREAMCUT,
                        CONNECTOR_READER_STREAM_INFO_END_STREAMCUT));
        for (Map<String, String> propsMap: streamPropsList) {
            String scopeInfo = descriptorProperties.getString(propsMap.get(CONNECTOR_READER_STREAM_INFO_SCOPE));
            String streamInfo = descriptorProperties.getString(propsMap.get(CONNECTOR_READER_STREAM_INFO_STREAM));
            String startCut = descriptorProperties.getString(propsMap.get(CONNECTOR_READER_STREAM_INFO_START_STREAMCUT));
            String endCut = descriptorProperties.getString(propsMap.get(CONNECTOR_READER_STREAM_INFO_END_STREAMCUT));
            Stream stream = Stream.of(scopeInfo, streamInfo);
            if (startCut != null && startCut.equals(StreamCut.UNBOUNDED.asText())) {
                startCut = null;
            }
            if (endCut != null && endCut.equals(StreamCut.UNBOUNDED.asText())) {
                endCut = null;
            }
            if (startCut == null && endCut != null) {
                throw new ValidationException("Must supply " + CONNECTOR_READER_STREAM_INFO_START_STREAMCUT + " if " + CONNECTOR_READER_STREAM_INFO_END_STREAMCUT + " is supplied");
            }
            if (startCut != null && endCut != null) {
                StreamCut startStreamCut = StreamCut.from(startCut);
                StreamCut endStreamCut = StreamCut.from(endCut);
                abstractReaderBuilder = abstractReaderBuilder.forStream(stream, startStreamCut, endStreamCut);
            } else if (startCut != null) {
                StreamCut startStreamCut = StreamCut.from(startCut);
                abstractReaderBuilder = abstractReaderBuilder.forStream(stream, startStreamCut);
            } else {
                abstractReaderBuilder = abstractReaderBuilder.forStream(stream);
            }

            abstractReaderBuilder = abstractReaderBuilder.withPravegaConfig(pravegaConfig);
        }

        tableSourceReaderBuilder.withDeserializationSchema(deserializationSchema);

        Supplier<FlinkPravegaReader<Row>> sourceFunctionFactory = abstractStreamingReaderBuilder::buildSourceFunction;
        Supplier<FlinkPravegaInputFormat<Row>> inputFormatFactory = tableSourceReaderBuilder::buildInputFormat;
        RowTypeInfo returnType = new RowTypeInfo(schema.getTypes(), schema.getColumnNames());

        FlinkPravegaTableSourceImpl flinkPravegaTableSourceImpl = new FlinkPravegaTableSourceImpl(sourceFunctionFactory, inputFormatFactory, schema, returnType);

        Optional<String> proctimeAttribute = SchemaValidator.deriveProctimeAttribute(descriptorProperties);
        if (proctimeAttribute.isPresent()) {
            flinkPravegaTableSourceImpl.setProctimeAttribute(proctimeAttribute.get());
        }

        List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors = SchemaValidator.deriveRowtimeAttributes(descriptorProperties);
        flinkPravegaTableSourceImpl.setRowtimeAttributeDescriptors(rowtimeAttributeDescriptors);

        return flinkPravegaTableSourceImpl;

    }

    public static class FlinkPravegaTableSourceImpl extends FlinkPravegaTableSource {

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
}
