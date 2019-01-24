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
import io.pravega.client.stream.impl.Credentials;
import io.pravega.connectors.flink.util.StreamWithBoundaries;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.net.URI;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.pravega.connectors.flink.Pravega.CONNECTOR_CONNECTION_CONFIG;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_CONNECTION_CONFIG_SECURITY;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_CONNECTION_CONFIG_SECURITY_AUTH_TYPE;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_CONNECTION_CONFIG_SECURITY_AUTH_TOKEN;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_CONNECTION_CONFIG_SECURITY_VALIDATE_HOSTNAME;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_CONNECTION_CONFIG_SECURITY_TRUST_STORE;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_CONNECTION_CONFIG_CONTROLLER_URI;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_CONNECTION_CONFIG_DEFAULT_SCOPE;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_READER;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_READER_READER_GROUP;
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

import static io.pravega.connectors.flink.Pravega.CONNECTOR_TYPE_VALUE_PRAVEGA;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_WRITER;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_WRITER_MODE;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_WRITER_MODE_VALUE_ATLEAST_ONCE;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_WRITER_MODE_VALUE_EXACTLY_ONCE;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_WRITER_ROUTING_KEY_FILED_NAME;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_WRITER_SCOPE;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_WRITER_STREAM;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_WRITER_TXN_LEASE_RENEWAL_INTERVAL;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_TIMESTAMPS_CLASS;
import static org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_TIMESTAMPS_FROM;
import static org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_TIMESTAMPS_SERIALIZED;
import static org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_TIMESTAMPS_TYPE;
import static org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_WATERMARKS_CLASS;
import static org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_WATERMARKS_DELAY;
import static org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_WATERMARKS_SERIALIZED;
import static org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_WATERMARKS_TYPE;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_FROM;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_PROCTIME;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_TYPE;

public abstract class FlinkPravegaTableSourceFactoryBase {

    protected Map<String, String> getRequiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR_TYPE(), CONNECTOR_TYPE_VALUE_PRAVEGA);
        context.put(CONNECTOR_PROPERTY_VERSION(), "1");
        context.put(CONNECTOR_VERSION(), getVersion());
        return context;
    }

    protected List<String> getSupportedProperties() {
        List<String> properties = new ArrayList<>();

        // pravega
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

        properties.add(CONNECTOR_WRITER);
        properties.add(CONNECTOR_WRITER_SCOPE);
        properties.add(CONNECTOR_WRITER_STREAM);
        properties.add(CONNECTOR_WRITER_MODE);
        properties.add(CONNECTOR_WRITER_TXN_LEASE_RENEWAL_INTERVAL);
        properties.add(CONNECTOR_WRITER_ROUTING_KEY_FILED_NAME);

        // schema
        properties.add(SCHEMA() + ".#." + SCHEMA_TYPE());
        properties.add(SCHEMA() + ".#." + SCHEMA_NAME());
        properties.add(SCHEMA() + ".#." + SCHEMA_FROM());

        // time attributes
        properties.add(SCHEMA() + ".#." + SCHEMA_PROCTIME());
        properties.add(SCHEMA() + ".#." + ROWTIME_TIMESTAMPS_TYPE());
        properties.add(SCHEMA() + ".#." + ROWTIME_TIMESTAMPS_FROM());
        properties.add(SCHEMA() + ".#." + ROWTIME_TIMESTAMPS_CLASS());
        properties.add(SCHEMA() + ".#." + ROWTIME_TIMESTAMPS_SERIALIZED());
        properties.add(SCHEMA() + ".#." + ROWTIME_WATERMARKS_TYPE());
        properties.add(SCHEMA() + ".#." + ROWTIME_WATERMARKS_CLASS());
        properties.add(SCHEMA() + ".#." + ROWTIME_WATERMARKS_SERIALIZED());
        properties.add(SCHEMA() + ".#." + ROWTIME_WATERMARKS_DELAY());

        // format wildcard
        properties.add(FORMAT() + ".*");

        return properties;
    }

    protected abstract String getVersion();

    protected DescriptorProperties getValidatedProperties(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);

        new SchemaValidator(true, false, false).validate(descriptorProperties);
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

    protected FlinkPravegaTableSource createFlinkPravegaTableSource(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        final TableSchema schema = descriptorProperties.getTableSchema(SCHEMA());
        final DeserializationSchema<Row> deserializationSchema = getDeserializationSchema(properties);

        ConnectorConfigurations connectorConfigurations = new ConnectorConfigurations();
        connectorConfigurations.parseConfigurations(descriptorProperties, ConnectorConfigurations.ConfigurationType.READER);

        Pravega.TableSourceReaderBuilder tableSourceReaderBuilder = new Pravega().tableSourceReaderBuilder();

        AbstractStreamingReaderBuilder abstractStreamingReaderBuilder = tableSourceReaderBuilder;
        if (connectorConfigurations.getUid().isPresent()) {
            abstractStreamingReaderBuilder.uid(connectorConfigurations.getUid().get());
        }
        if (connectorConfigurations.getRgScope().isPresent()) {
            abstractStreamingReaderBuilder.withReaderGroupScope(connectorConfigurations.getRgScope().get());
        }
        if (connectorConfigurations.getRgName().isPresent()) {
            abstractStreamingReaderBuilder.withReaderGroupName(connectorConfigurations.getRgName().get());
        }
        if (connectorConfigurations.getRefreshInterval().isPresent()) {
            abstractStreamingReaderBuilder.withReaderGroupRefreshTime(Time.milliseconds(connectorConfigurations.getRefreshInterval().get()));
        }
        if (connectorConfigurations.getEventReadTimeoutInterval().isPresent()) {
            abstractStreamingReaderBuilder.withEventReadTimeout(Time.milliseconds(connectorConfigurations.getEventReadTimeoutInterval().get()));
        }
        if (connectorConfigurations.getCheckpointInitiateTimeoutInterval().isPresent()) {
            abstractStreamingReaderBuilder.withCheckpointInitiateTimeout(Time.milliseconds(connectorConfigurations.getCheckpointInitiateTimeoutInterval().get()));
        }

        AbstractReaderBuilder abstractReaderBuilder = abstractStreamingReaderBuilder;

        abstractReaderBuilder.withPravegaConfig(connectorConfigurations.getPravegaConfig());

        if (connectorConfigurations.getMetrics().isPresent()) {
            abstractReaderBuilder.enableMetrics(connectorConfigurations.getMetrics().get());
        }

        abstractReaderBuilder.withPravegaConfig(connectorConfigurations.getPravegaConfig());

        for (StreamWithBoundaries streamWithBoundaries: connectorConfigurations.getReaderStreams()) {
            if (streamWithBoundaries.getFrom() != StreamCut.UNBOUNDED && streamWithBoundaries.getTo() != StreamCut.UNBOUNDED) {
                abstractReaderBuilder.forStream(streamWithBoundaries.getStream(), streamWithBoundaries.getFrom(), streamWithBoundaries.getTo());
            } else if (streamWithBoundaries.getFrom() != StreamCut.UNBOUNDED) {
                abstractReaderBuilder.forStream(streamWithBoundaries.getStream(), streamWithBoundaries.getFrom());
            } else {
                abstractReaderBuilder.forStream(streamWithBoundaries.getStream());
            }
        }

        tableSourceReaderBuilder.withDeserializationSchema(deserializationSchema);

        FlinkPravegaStreamTableSourceFactory.FlinkPravegaTableSourceImpl flinkPravegaTableSourceImpl =
                new FlinkPravegaStreamTableSourceFactory.FlinkPravegaTableSourceImpl(abstractStreamingReaderBuilder::buildSourceFunction,
                        tableSourceReaderBuilder::buildInputFormat, schema, new RowTypeInfo(schema.getTypes(), schema.getColumnNames()));

        flinkPravegaTableSourceImpl.setRowtimeAttributeDescriptors(SchemaValidator.deriveRowtimeAttributes(descriptorProperties));

        Optional<String> procTimeAttribute = SchemaValidator.deriveProctimeAttribute(descriptorProperties);
        if (procTimeAttribute.isPresent()) {
            flinkPravegaTableSourceImpl.setProctimeAttribute(procTimeAttribute.get());
        }

        return flinkPravegaTableSourceImpl;
    }

    protected FlinkPravegaTableSink createFlinkPravegaTableSink(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        final TableSchema schema = descriptorProperties.getTableSchema(SCHEMA());
        SerializationSchema<Row> serializationSchema = getSerializationSchema(properties);

        ConnectorConfigurations connectorConfigurations = new ConnectorConfigurations();
        connectorConfigurations.parseConfigurations(descriptorProperties, ConnectorConfigurations.ConfigurationType.WRITER);

        Pravega.TableSinkWriterBuilder tableSinkWriterBuilder = new Pravega().tableSinkWriterBuilder();
        AbstractWriterBuilder abstractWriterBuilder = tableSinkWriterBuilder;

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
        tableSinkWriterBuilder.withRoutingKeyField(connectorConfigurations.getRoutingKey());
        tableSinkWriterBuilder.withSerializationSchema(serializationSchema);
        abstractWriterBuilder.forStream(connectorConfigurations.getWriterStream());
        abstractWriterBuilder.withPravegaConfig(connectorConfigurations.getPravegaConfig());

        return new FlinkPravegaStreamTableSinkFactory.FlinkPravegaTableSinkImpl(tableSinkWriterBuilder::createSinkFunction,
                tableSinkWriterBuilder::createOutputFormat)
                .configure(schema.getColumnNames(), schema.getTypes());
    }


    @Data
    public static final class ConnectorConfigurations {

        private Optional<Boolean> metrics;

        // connection config
        private String controllerUri;
        private Optional<String> defaultScope;

        // security
        private Optional<String> authType;
        private Optional<String> authToken;
        private Optional<Boolean> validateHostName;
        private Optional<String> trustStore;

        // reader group
        private Optional<String> uid;
        private Optional<String> rgScope;
        private Optional<String> rgName;
        private Optional<Long> refreshInterval;
        private Optional<Long> eventReadTimeoutInterval;
        private Optional<Long> checkpointInitiateTimeoutInterval;

        // reader stream info
        private List<StreamWithBoundaries> readerStreams = new ArrayList<>();

        // writer info
        private Stream writerStream;
        private Optional<PravegaWriterMode> writerMode;
        private Optional<Long> txnLeaseRenewalInterval;
        private String routingKey;

        private PravegaConfig pravegaConfig;

        public void parseConfigurations(DescriptorProperties descriptorProperties, ConfigurationType configurationType) {
            metrics =  descriptorProperties.getOptionalBoolean(CONNECTOR_METRICS);
            controllerUri = descriptorProperties.getString(CONNECTOR_CONNECTION_CONFIG_CONTROLLER_URI);
            defaultScope = descriptorProperties.getOptionalString(CONNECTOR_CONNECTION_CONFIG_DEFAULT_SCOPE);

            authType = descriptorProperties.getOptionalString(CONNECTOR_CONNECTION_CONFIG_SECURITY_AUTH_TYPE);
            authToken = descriptorProperties.getOptionalString(CONNECTOR_CONNECTION_CONFIG_SECURITY_AUTH_TOKEN);
            validateHostName =  descriptorProperties.getOptionalBoolean(CONNECTOR_CONNECTION_CONFIG_SECURITY_VALIDATE_HOSTNAME);
            trustStore = descriptorProperties.getOptionalString(CONNECTOR_CONNECTION_CONFIG_SECURITY_TRUST_STORE);

            createPravegaConfig();

            if (configurationType == ConfigurationType.READER) {
                uid = descriptorProperties.getOptionalString(CONNECTOR_READER_READER_GROUP_UID);
                rgScope = descriptorProperties.getOptionalString(CONNECTOR_READER_READER_GROUP_SCOPE);
                rgName = descriptorProperties.getOptionalString(CONNECTOR_READER_READER_GROUP_NAME);
                refreshInterval = descriptorProperties.getOptionalLong(CONNECTOR_READER_READER_GROUP_REFRESH_INTERVAL);
                eventReadTimeoutInterval = descriptorProperties.getOptionalLong(CONNECTOR_READER_READER_GROUP_EVENT_READ_TIMEOUT_INTERVAL);
                checkpointInitiateTimeoutInterval = descriptorProperties.getOptionalLong(CONNECTOR_READER_READER_GROUP_CHECKPOINT_INITIATE_TIMEOUT_INTERVAL);

                if (!defaultScope.isPresent() && !rgScope.isPresent()) {
                    throw new ValidationException("Must supply either " + CONNECTOR_READER_READER_GROUP_SCOPE + " or " + CONNECTOR_CONNECTION_CONFIG_DEFAULT_SCOPE);
                }

                final List<Map<String, String>> streamPropsList = descriptorProperties.getVariableIndexedProperties(
                        CONNECTOR_READER_STREAM_INFO,
                        Arrays.asList(CONNECTOR_READER_STREAM_INFO_STREAM));
                if (streamPropsList.isEmpty()) {
                    throw new ValidationException(CONNECTOR_READER_STREAM_INFO + " cannot be empty");
                }
                for (Map<String, String> propsMap : streamPropsList) {
                    if (!propsMap.containsKey(CONNECTOR_READER_STREAM_INFO_SCOPE) && !defaultScope.isPresent()) {
                        throw new ValidationException("Must supply either " + CONNECTOR_READER_STREAM_INFO + "." + CONNECTOR_READER_STREAM_INFO_SCOPE +
                                " or " + CONNECTOR_CONNECTION_CONFIG_DEFAULT_SCOPE);
                    }
                    String scopeName = (propsMap.containsKey(CONNECTOR_READER_STREAM_INFO_SCOPE)) ?
                            descriptorProperties.getString(propsMap.get(CONNECTOR_READER_STREAM_INFO_SCOPE)) : defaultScope.get();

                    if (!propsMap.containsKey(CONNECTOR_READER_STREAM_INFO_STREAM)) {
                        throw new ValidationException(CONNECTOR_READER_STREAM_INFO + "." +  CONNECTOR_READER_STREAM_INFO_STREAM + " cannot be empty");
                    }
                    String streamName = descriptorProperties.getString(propsMap.get(CONNECTOR_READER_STREAM_INFO_STREAM));

                    String startCut = StreamCut.UNBOUNDED.asText();
                    if (propsMap.containsKey(CONNECTOR_READER_STREAM_INFO_START_STREAMCUT)) {
                        startCut = descriptorProperties.getString(propsMap.get(CONNECTOR_READER_STREAM_INFO_START_STREAMCUT));
                    }

                    String endCut = StreamCut.UNBOUNDED.asText();
                    if (propsMap.containsKey(CONNECTOR_READER_STREAM_INFO_END_STREAMCUT)) {
                        endCut = descriptorProperties.getString(propsMap.get(CONNECTOR_READER_STREAM_INFO_END_STREAMCUT));
                    }

                    Stream stream = Stream.of(scopeName, streamName);
                    readerStreams.add(new StreamWithBoundaries(stream, StreamCut.from(startCut), StreamCut.from(endCut)));
                }
            }

            if (configurationType == ConfigurationType.WRITER) {
                Optional<String> streamScope = descriptorProperties.getOptionalString(CONNECTOR_WRITER_SCOPE);

                if (!defaultScope.isPresent() && !streamScope.isPresent()) {
                    throw new ValidationException("Must supply either " + CONNECTOR_WRITER_SCOPE + " or " + CONNECTOR_CONNECTION_CONFIG_DEFAULT_SCOPE);
                }

                final String scopeVal = streamScope.isPresent() ? streamScope.get() : defaultScope.get();

                if (!descriptorProperties.containsKey(CONNECTOR_WRITER_STREAM)) {
                    throw new ValidationException("Missing " + CONNECTOR_WRITER_STREAM + " configuration.");
                }
                final String streamName = descriptorProperties.getString(CONNECTOR_WRITER_STREAM);
                writerStream = Stream.of(scopeVal, streamName);

                txnLeaseRenewalInterval = descriptorProperties.getOptionalLong(CONNECTOR_WRITER_TXN_LEASE_RENEWAL_INTERVAL);

                if (!descriptorProperties.containsKey(CONNECTOR_WRITER_ROUTING_KEY_FILED_NAME)) {
                    throw new ValidationException("Missing " + CONNECTOR_WRITER_ROUTING_KEY_FILED_NAME + " configuration.");
                }
                routingKey = descriptorProperties.getString(CONNECTOR_WRITER_ROUTING_KEY_FILED_NAME);

                Optional<String> optionalMode = descriptorProperties.getOptionalString(CONNECTOR_WRITER_MODE);
                if (optionalMode.isPresent()) {
                  String mode = optionalMode.get();
                  if (mode.equals(CONNECTOR_WRITER_MODE_VALUE_ATLEAST_ONCE)) {
                      writerMode = Optional.of(PravegaWriterMode.ATLEAST_ONCE);
                  } else if (mode.equals(CONNECTOR_WRITER_MODE_VALUE_EXACTLY_ONCE)) {
                      writerMode = Optional.of(PravegaWriterMode.EXACTLY_ONCE);
                  } else {
                      throw new ValidationException("Invalid writer mode " + mode + " passed. Supported values: ("
                              + CONNECTOR_WRITER_MODE_VALUE_ATLEAST_ONCE + " or " + CONNECTOR_WRITER_MODE_VALUE_EXACTLY_ONCE + ")");
                  }
                }
            }
        }

        private void createPravegaConfig() {
            pravegaConfig = PravegaConfig.fromDefaults()
                    .withControllerURI(URI.create(controllerUri));

            if (defaultScope.isPresent()) {
                pravegaConfig.withDefaultScope(defaultScope.get());
            }
            if (authType.isPresent() && authToken.isPresent()) {
                pravegaConfig.withCredentials(new SimpleCredentials(authType.get(), authToken.get()));
            }
            if (validateHostName.isPresent()) {
                pravegaConfig = pravegaConfig.withHostnameValidation(validateHostName.get());
            }
            if (trustStore.isPresent()) {
                pravegaConfig = pravegaConfig.withTrustStore(trustStore.get());
            }
        }

        enum ConfigurationType {
            READER,
            WRITER
        }
    }

    @Data
    @EqualsAndHashCode
    public static class SimpleCredentials implements Credentials {

        private final String authType;

        private final String authToken;

        @Override
        public String getAuthenticationType() {
            return authType;
        }

        @Override
        public String getAuthenticationToken() {
            return authToken;
        }
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
