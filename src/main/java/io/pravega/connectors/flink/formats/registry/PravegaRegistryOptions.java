/**
 * Copyright Pravega Authors.
 *
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

package io.pravega.connectors.flink.formats.registry;

import io.pravega.connectors.flink.dynamic.table.PravegaOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.protobuf.PbFormatOptions;

public class PravegaRegistryOptions {

        public static final ConfigOption<String> URI = ConfigOptions
                        .key("uri")
                        .stringType()
                        .noDefaultValue()
                        .withDescription("Required URI of Pravega schema registry");

        public static final ConfigOption<String> NAMESPACE = ConfigOptions
                        .key("namespace")
                        .stringType()
                        .noDefaultValue()
                        .withDescription(
                                        "Required Pravega schema registry's namespace, should be the same as the Pravega scope name");

        public static final ConfigOption<String> GROUP_ID = ConfigOptions
                        .key("group-id")
                        .stringType()
                        .noDefaultValue()
                        .withDescription(
                                        "Required Pravega schema registry's groupID, should be the same as the Pravega stream name");

        public static final ConfigOption<String> FORMAT = ConfigOptions
                        .key("format")
                        .stringType()
                        .defaultValue("Avro")
                        .withDescription(
                                        "Optional serialization format for Pravega catalog. Valid enumerations are ['Avro'(default), 'Json']");

        // Pravega security options
        public static final ConfigOption<String> SECURITY_AUTH_TYPE = PravegaOptions.SECURITY_AUTH_TYPE;
        public static final ConfigOption<String> SECURITY_AUTH_TOKEN = PravegaOptions.SECURITY_AUTH_TOKEN;
        public static final ConfigOption<Boolean> SECURITY_VALIDATE_HOSTNAME = PravegaOptions.SECURITY_VALIDATE_HOSTNAME;
        public static final ConfigOption<String> SECURITY_TRUST_STORE = PravegaOptions.SECURITY_TRUST_STORE;

        // --------------------------------------------------------------------------------------------
        // Json Options
        // --------------------------------------------------------------------------------------------

        public static final ConfigOption<Boolean> FAIL_ON_MISSING_FIELD = JsonFormatOptions.FAIL_ON_MISSING_FIELD;
        public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS = JsonFormatOptions.IGNORE_PARSE_ERRORS;
        public static final ConfigOption<String> TIMESTAMP_FORMAT = JsonFormatOptions.TIMESTAMP_FORMAT;
        public static final ConfigOption<String> MAP_NULL_KEY_MODE = JsonFormatOptions.MAP_NULL_KEY_MODE;
        public static final ConfigOption<String> MAP_NULL_KEY_LITERAL = JsonFormatOptions.MAP_NULL_KEY_LITERAL;
        public static final ConfigOption<Boolean> ENCODE_DECIMAL_AS_PLAIN_NUMBER = JsonFormatOptions.ENCODE_DECIMAL_AS_PLAIN_NUMBER;

        // --------------------------------------------------------------------------------------------
        // Protobuf Options
        // --------------------------------------------------------------------------------------------

        public static final ConfigOption<String> PB_MESSAGE_CLASS_NAME = PbFormatOptions.MESSAGE_CLASS_NAME;
        public static final ConfigOption<Boolean> PB_IGNORE_PARSE_ERRORS = PbFormatOptions.IGNORE_PARSE_ERRORS;
        public static final ConfigOption<Boolean> PB_READ_DEFAULT_VALUES = PbFormatOptions.READ_DEFAULT_VALUES;
        public static final ConfigOption<String> PB_WRITE_NULL_STRING_LITERAL = PbFormatOptions.WRITE_NULL_STRING_LITERAL;
}
