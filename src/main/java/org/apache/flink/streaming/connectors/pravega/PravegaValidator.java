/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.streaming.connectors.pravega;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Pravega descriptor validation implementation for validating the Pravega reader and writer configurations.
 */
public class PravegaValidator extends ConnectorDescriptorValidator {

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
        properties.validateValue(CONNECTOR_TYPE, Pravega.CONNECTOR_TYPE_VALUE_PRAVEGA, false);
        validateConnectionConfig(properties);
        if (properties.containsKey(Pravega.CONNECTOR_READER)) {
            validateReaderConfigurations(properties);
        }
        if (properties.containsKey(Pravega.CONNECTOR_WRITER)) {
            validateWriterConfigurations(properties);
        }
    }

    private void validateConnectionConfig(DescriptorProperties properties) {
        properties.validateString(Pravega.CONNECTOR_CONNECTION_CONFIG_CONTROLLER_URI, false, 1, Integer.MAX_VALUE);
    }

    private void validateReaderConfigurations(DescriptorProperties properties) {
        final Map<String, Consumer<String>> streamPropertyValidators = new HashMap<>();
        streamPropertyValidators.put(
                Pravega.CONNECTOR_READER_STREAM_INFO_SCOPE,
                prefix -> properties.validateString(prefix, true, 1));
        streamPropertyValidators.put(
                Pravega.CONNECTOR_READER_STREAM_INFO_STREAM,
                prefix -> properties.validateString(prefix, false, 0));
        streamPropertyValidators.put(
                Pravega.CONNECTOR_READER_STREAM_INFO_START_STREAMCUT,
                prefix -> properties.validateString(prefix, true, 1));
        streamPropertyValidators.put(
                Pravega.CONNECTOR_READER_STREAM_INFO_END_STREAMCUT,
                prefix -> properties.validateString(prefix, true, 1));
        properties.validateFixedIndexedProperties(Pravega.CONNECTOR_READER_STREAM_INFO, true, streamPropertyValidators);

        // for readers we need default-scope from connection config or reader group scope
        Optional<String> readerGroupScope = properties.getOptionalString(Pravega.CONNECTOR_READER_READER_GROUP_SCOPE);
        Optional<String> connConfigDefaultScope = properties.getOptionalString(Pravega.CONNECTOR_CONNECTION_CONFIG_DEFAULT_SCOPE);
        if (!readerGroupScope.isPresent() && !connConfigDefaultScope.isPresent()) {
            throw new ValidationException("Must supply either " + Pravega.CONNECTOR_READER_READER_GROUP_SCOPE + " or " + Pravega.CONNECTOR_CONNECTION_CONFIG_DEFAULT_SCOPE);
        }
    }

    private void validateWriterConfigurations(DescriptorProperties properties) {
        properties.validateString(Pravega.CONNECTOR_WRITER_SCOPE, true, 1, Integer.MAX_VALUE);
        properties.validateString(Pravega.CONNECTOR_WRITER_STREAM, false, 1, Integer.MAX_VALUE);
        properties.validateString(Pravega.CONNECTOR_WRITER_ROUTING_KEY_FILED_NAME, false, 1, Integer.MAX_VALUE);

        // for writers we need default-scope from connection config or scope from writer config
        Optional<String> scope = properties.getOptionalString(Pravega.CONNECTOR_WRITER_SCOPE);
        Optional<String> connConfigDefaultScope = properties.getOptionalString(Pravega.CONNECTOR_CONNECTION_CONFIG_DEFAULT_SCOPE);
        if (!scope.isPresent() && !connConfigDefaultScope.isPresent()) {
            throw new ValidationException("Must supply either " + Pravega.CONNECTOR_WRITER_SCOPE + " or " + Pravega.CONNECTOR_CONNECTION_CONFIG_DEFAULT_SCOPE);
        }

        // validate writer mode
        final Map<String, Consumer<String>> writerModeValidators = new HashMap<>();
        writerModeValidators.put(Pravega.CONNECTOR_WRITER_MODE_VALUE_EXACTLY_ONCE, properties.noValidation());
        writerModeValidators.put(Pravega.CONNECTOR_WRITER_MODE_VALUE_ATLEAST_ONCE, properties.noValidation());
        properties.validateEnum(Pravega.CONNECTOR_WRITER_MODE, true, writerModeValidators);
    }

}