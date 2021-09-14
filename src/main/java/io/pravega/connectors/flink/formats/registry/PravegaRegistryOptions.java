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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.formats.json.JsonOptions;

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
            .withDescription("Required Pravega schema registry's namespace, should be the same as the Pravega scope name");

    public static final ConfigOption<String> GROUP_ID = ConfigOptions
            .key("group-id")
            .stringType()
            .noDefaultValue()
            .withDescription("Required Pravega schema registry's groupID, should be the same as the Pravega stream name");

    public static final ConfigOption<String> FORMAT = ConfigOptions
            .key("format")
            .stringType()
            .defaultValue("Avro")
            .withDescription("Optional serialization format for Pravega catalog. Valid enumerations are ['Avro'(default), 'Json']");

    // --------------------------------------------------------------------------------------------
    // Json Options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<Boolean> FAIL_ON_MISSING_FIELD = JsonOptions.FAIL_ON_MISSING_FIELD;
    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS = JsonOptions.IGNORE_PARSE_ERRORS;
    public static final ConfigOption<String> TIMESTAMP_FORMAT = JsonOptions.TIMESTAMP_FORMAT;
    public static final ConfigOption<String> MAP_NULL_KEY_MODE = JsonOptions.MAP_NULL_KEY_MODE;
    public static final ConfigOption<String> MAP_NULL_KEY_LITERAL = JsonOptions.MAP_NULL_KEY_LITERAL;
    public static final ConfigOption<Boolean> ENCODE_DECIMAL_AS_PLAIN_NUMBER = JsonOptions.ENCODE_DECIMAL_AS_PLAIN_NUMBER;
}
