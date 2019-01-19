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
import lombok.Data;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.types.Row;

import java.net.URI;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.pravega.connectors.flink.Pravega.*;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_APPEND;
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

public class FlinkPravegaTableSourceFactoryBase {

    protected Map<String, String> getRequiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR_TYPE(), CONNECTOR_TYPE_VALUE_PRAVEGA);
        context.put(CONNECTOR_PROPERTY_VERSION(), String.valueOf(CONNECTOR_VERSION_VALUE));
        context.put(UPDATE_MODE(), UPDATE_MODE_VALUE_APPEND());
        context.put(CONNECTOR_VERSION(), String.valueOf(CONNECTOR_VERSION_VALUE));
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

                final String scope = defaultScope.isPresent() ? defaultScope.get() : rgScope.get();

                createPravegaConfig(scope);

                final List<Map<String, String>> streamPropsList = descriptorProperties.getFixedIndexedProperties(
                        CONNECTOR_READER_STREAM_INFO,
                        Arrays.asList(CONNECTOR_READER_STREAM_INFO_SCOPE,
                                CONNECTOR_READER_STREAM_INFO_STREAM,
                                CONNECTOR_READER_STREAM_INFO_START_STREAMCUT,
                                CONNECTOR_READER_STREAM_INFO_END_STREAMCUT));
                for (Map<String, String> propsMap : streamPropsList) {
                    String scopeInfo = descriptorProperties.getString(propsMap.get(CONNECTOR_READER_STREAM_INFO_SCOPE));
                    String streamInfo = descriptorProperties.getString(propsMap.get(CONNECTOR_READER_STREAM_INFO_STREAM));
                    String startCut = descriptorProperties.getString(propsMap.get(CONNECTOR_READER_STREAM_INFO_START_STREAMCUT));
                    String endCut = descriptorProperties.getString(propsMap.get(CONNECTOR_READER_STREAM_INFO_END_STREAMCUT));
                    Stream stream = Stream.of(scopeInfo, streamInfo);
                    readerStreams.add(new StreamWithBoundaries(stream, StreamCut.from(startCut), StreamCut.from(endCut)));
                }
            } else if (configurationType == ConfigurationType.WRITER) {
                Optional<String> streamScope = descriptorProperties.getOptionalString(CONNECTOR_WRITER_SCOPE);

                if (!defaultScope.isPresent() && !streamScope.isPresent()) {
                    throw new ValidationException("Must supply either " + CONNECTOR_WRITER_SCOPE + " or " + CONNECTOR_CONNECTION_CONFIG_DEFAULT_SCOPE);
                }

                final String scope = defaultScope.isPresent() ? defaultScope.get() : streamScope.get();
                createPravegaConfig(scope);

                final String scopeVal = streamScope.isPresent() ? streamScope.get() : defaultScope.get();
                final String streamName = descriptorProperties.getString(CONNECTOR_WRITER_STREAM);
                writerStream = Stream.of(scopeVal, streamName);

                txnLeaseRenewalInterval = descriptorProperties.getOptionalLong(CONNECTOR_WRITER_TXN_LEASE_RENEWAL_INTERVAL);
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

        private void createPravegaConfig(final String scope) {
            //TODO get auth type and auth token and create a new Security impl instance
            pravegaConfig = PravegaConfig.fromDefaults()
                    .withControllerURI(URI.create(controllerUri))
                    .withDefaultScope(scope)
                    .withCredentials(new DefaultCredentials("1111_aaaa", "admin"));
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

}
