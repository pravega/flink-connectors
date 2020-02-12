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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static io.pravega.connectors.flink.Pravega.*;

/**
 * Pravega descriptor validation implementation for validating the Pravega reader and writer configurations.
 */
public class PravegaValidator extends ConnectorDescriptorValidator {

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
        properties.validateValue(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_PRAVEGA, false);
        validateConnectionConfig(properties);
        if (properties.containsKey(CONNECTOR_READER)) {
            validateReaderConfigurations(properties);
        }
        if (properties.containsKey(CONNECTOR_WRITER)) {
            validateWriterConfigurations(properties);
        }
    }

    private void validateConnectionConfig(DescriptorProperties properties) {
        properties.validateString(CONNECTOR_CONNECTION_CONFIG_CONTROLLER_URI, false, 1, Integer.MAX_VALUE);
    }

    private void validateReaderConfigurations(DescriptorProperties properties) {
        final Map<String, Consumer<String>> streamPropertyValidators = new HashMap<>();
        streamPropertyValidators.put(
                CONNECTOR_READER_STREAM_INFO_SCOPE,
                prefix -> properties.validateString(prefix, true, 1));
        streamPropertyValidators.put(
                CONNECTOR_READER_STREAM_INFO_STREAM,
                prefix -> properties.validateString(prefix, false, 0));
        streamPropertyValidators.put(
                CONNECTOR_READER_STREAM_INFO_START_STREAMCUT,
                prefix -> properties.validateString(prefix, true, 1));
        streamPropertyValidators.put(
                CONNECTOR_READER_STREAM_INFO_END_STREAMCUT,
                prefix -> properties.validateString(prefix, true, 1));
        properties.validateFixedIndexedProperties(CONNECTOR_READER_STREAM_INFO, true, streamPropertyValidators);

        // for readers we need default-scope from connection config or reader group scope
        Optional<String> readerGroupScope = properties.getOptionalString(CONNECTOR_READER_READER_GROUP_SCOPE);
        Optional<String> connConfigDefaultScope = properties.getOptionalString(CONNECTOR_CONNECTION_CONFIG_DEFAULT_SCOPE);
        if (!readerGroupScope.isPresent() && !connConfigDefaultScope.isPresent()) {
            throw new ValidationException("Must supply either " + CONNECTOR_READER_READER_GROUP_SCOPE + " or " + CONNECTOR_CONNECTION_CONFIG_DEFAULT_SCOPE);
        }
    }

    private void validateWriterConfigurations(DescriptorProperties properties) {
        properties.validateString(CONNECTOR_WRITER_SCOPE, true, 1, Integer.MAX_VALUE);
        properties.validateString(CONNECTOR_WRITER_STREAM, false, 1, Integer.MAX_VALUE);
        properties.validateString(CONNECTOR_WRITER_ROUTING_KEY_FILED_NAME, false, 1, Integer.MAX_VALUE);

        // for writers we need default-scope from connection config or scope from writer config
        Optional<String> scope = properties.getOptionalString(CONNECTOR_WRITER_SCOPE);
        Optional<String> connConfigDefaultScope = properties.getOptionalString(CONNECTOR_CONNECTION_CONFIG_DEFAULT_SCOPE);
        if (!scope.isPresent() && !connConfigDefaultScope.isPresent()) {
            throw new ValidationException("Must supply either " + CONNECTOR_WRITER_SCOPE + " or " + CONNECTOR_CONNECTION_CONFIG_DEFAULT_SCOPE);
        }

        // validate writer mode
        final Map<String, Consumer<String>> writerModeValidators = new HashMap<>();
        writerModeValidators.put(CONNECTOR_WRITER_MODE_VALUE_EXACTLY_ONCE, properties.noValidation());
        writerModeValidators.put(CONNECTOR_WRITER_MODE_VALUE_ATLEAST_ONCE, properties.noValidation());
        properties.validateEnum(CONNECTOR_WRITER_MODE, true, writerModeValidators);
    }

}