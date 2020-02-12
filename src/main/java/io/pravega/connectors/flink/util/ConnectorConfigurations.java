/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink.util;

import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.Credentials;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.PravegaWriterMode;
import io.pravega.connectors.flink.watermark.AssignerWithTimeWindows;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.pravega.connectors.flink.Pravega.CONNECTOR_CONNECTION_CONFIG_CONTROLLER_URI;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_CONNECTION_CONFIG_DEFAULT_SCOPE;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_CONNECTION_CONFIG_SECURITY_AUTH_TOKEN;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_CONNECTION_CONFIG_SECURITY_AUTH_TYPE;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_CONNECTION_CONFIG_SECURITY_TRUST_STORE;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_CONNECTION_CONFIG_SECURITY_VALIDATE_HOSTNAME;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_METRICS;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_READER_READER_GROUP_CHECKPOINT_INITIATE_TIMEOUT_INTERVAL;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_READER_READER_GROUP_EVENT_READ_TIMEOUT_INTERVAL;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_READER_READER_GROUP_NAME;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_READER_READER_GROUP_REFRESH_INTERVAL;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_READER_READER_GROUP_SCOPE;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_READER_READER_GROUP_UID;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_READER_STREAM_INFO;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_READER_STREAM_INFO_END_STREAMCUT;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_READER_STREAM_INFO_SCOPE;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_READER_STREAM_INFO_START_STREAMCUT;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_READER_STREAM_INFO_STREAM;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_READER_USER_TIMESTAMP_ASSIGNER;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_WRITER_ENABLE_WATERMARK;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_WRITER_MODE;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_WRITER_MODE_VALUE_ATLEAST_ONCE;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_WRITER_MODE_VALUE_EXACTLY_ONCE;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_WRITER_ROUTING_KEY_FILED_NAME;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_WRITER_SCOPE;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_WRITER_STREAM;
import static io.pravega.connectors.flink.Pravega.CONNECTOR_WRITER_TXN_LEASE_RENEWAL_INTERVAL;

/**
 * Pravega connector configurations used to parse and map the {@link DescriptorProperties}.
 */
@Data
public final class ConnectorConfigurations {

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

    // reader user info
    private Optional<AssignerWithTimeWindows<Row>> assignerWithTimeWindows;

    // writer info
    private Stream writerStream;
    private Optional<PravegaWriterMode> writerMode;
    private Optional<Long> txnLeaseRenewalInterval;
    private Boolean watermark;
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
            populateReaderConfig(descriptorProperties);
        }

        if (configurationType == ConfigurationType.WRITER) {
            populateWriterConfig(descriptorProperties);
        }
    }

    @SuppressWarnings("unchecked")
    private void populateReaderConfig(DescriptorProperties descriptorProperties) {
        uid = descriptorProperties.getOptionalString(CONNECTOR_READER_READER_GROUP_UID);
        rgScope = descriptorProperties.getOptionalString(CONNECTOR_READER_READER_GROUP_SCOPE);
        rgName = descriptorProperties.getOptionalString(CONNECTOR_READER_READER_GROUP_NAME);
        refreshInterval = descriptorProperties.getOptionalLong(CONNECTOR_READER_READER_GROUP_REFRESH_INTERVAL);
        eventReadTimeoutInterval = descriptorProperties.getOptionalLong(CONNECTOR_READER_READER_GROUP_EVENT_READ_TIMEOUT_INTERVAL);
        checkpointInitiateTimeoutInterval = descriptorProperties.getOptionalLong(CONNECTOR_READER_READER_GROUP_CHECKPOINT_INITIATE_TIMEOUT_INTERVAL);

        final Optional<Class<AssignerWithTimeWindows>> assignerClass = descriptorProperties.getOptionalClass(
                CONNECTOR_READER_USER_TIMESTAMP_ASSIGNER, AssignerWithTimeWindows.class);
        if (assignerClass.isPresent()) {
            assignerWithTimeWindows = Optional.of((AssignerWithTimeWindows<Row>) InstantiationUtil.instantiate(assignerClass.get()));
        } else {
            assignerWithTimeWindows = Optional.empty();
        }

        if (!defaultScope.isPresent() && !rgScope.isPresent()) {
            throw new ValidationException("Must supply either " + CONNECTOR_READER_READER_GROUP_SCOPE + " or " + CONNECTOR_CONNECTION_CONFIG_DEFAULT_SCOPE);
        }

        final List<Map<String, String>> streamPropsList = descriptorProperties.getVariableIndexedProperties(
                CONNECTOR_READER_STREAM_INFO,
                Arrays.asList(CONNECTOR_READER_STREAM_INFO_STREAM));

        if (streamPropsList.isEmpty()) {
            throw new ValidationException(CONNECTOR_READER_STREAM_INFO + " cannot be empty");
        }

        int index = 0;
        for (Map<String, String> propsMap : streamPropsList) {
            if (!propsMap.containsKey(CONNECTOR_READER_STREAM_INFO_SCOPE) && !defaultScope.isPresent()) {
                throw new ValidationException("Must supply either " + CONNECTOR_READER_STREAM_INFO + "." + index + "." + CONNECTOR_READER_STREAM_INFO_SCOPE +
                        " or " + CONNECTOR_CONNECTION_CONFIG_DEFAULT_SCOPE);
            }
            String scopeName = (propsMap.containsKey(CONNECTOR_READER_STREAM_INFO_SCOPE)) ?
                    descriptorProperties.getString(propsMap.get(CONNECTOR_READER_STREAM_INFO_SCOPE)) : defaultScope.get();

            if (!propsMap.containsKey(CONNECTOR_READER_STREAM_INFO_STREAM)) {
                throw new ValidationException(CONNECTOR_READER_STREAM_INFO + "." + index + "." +  CONNECTOR_READER_STREAM_INFO_STREAM + " cannot be empty");
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
            index++;
        }
    }

    private void populateWriterConfig(DescriptorProperties descriptorProperties) {
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
        watermark = descriptorProperties.getBoolean(CONNECTOR_WRITER_ENABLE_WATERMARK);
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

    public enum ConfigurationType {
        READER,
        WRITER
    }

    @Data
    @EqualsAndHashCode
    public static final class SimpleCredentials implements Credentials {

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
}