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

import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.dynamic.table.PravegaOptions;
import io.pravega.connectors.flink.util.FlinkPravegaUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.json.JsonOptions;

import java.util.Optional;

import static io.pravega.connectors.flink.util.FlinkPravegaUtils.isCredentialsLoadDynamic;

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

    // Pravega security options
    public static final ConfigOption<String> SECURITY_AUTH_TYPE = PravegaOptions.SECURITY_AUTH_TYPE;
    public static final ConfigOption<String> SECURITY_AUTH_TOKEN = PravegaOptions.SECURITY_AUTH_TOKEN;
    public static final ConfigOption<Boolean> SECURITY_VALIDATE_HOSTNAME = PravegaOptions.SECURITY_VALIDATE_HOSTNAME;
    public static final ConfigOption<String> SECURITY_TRUST_STORE = PravegaOptions.SECURITY_TRUST_STORE;

    // --------------------------------------------------------------------------------------------
    // Json Options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<Boolean> FAIL_ON_MISSING_FIELD = JsonOptions.FAIL_ON_MISSING_FIELD;
    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS = JsonOptions.IGNORE_PARSE_ERRORS;
    public static final ConfigOption<String> TIMESTAMP_FORMAT = JsonOptions.TIMESTAMP_FORMAT;
    public static final ConfigOption<String> MAP_NULL_KEY_MODE = JsonOptions.MAP_NULL_KEY_MODE;
    public static final ConfigOption<String> MAP_NULL_KEY_LITERAL = JsonOptions.MAP_NULL_KEY_LITERAL;

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    public static PravegaConfig getPravegaConfig(ReadableConfig tableOptions) {
        PravegaConfig pravegaConfig = PravegaConfig.fromDefaults()
                .withDefaultScope(tableOptions.get(NAMESPACE))
                .withHostnameValidation(tableOptions.get(SECURITY_VALIDATE_HOSTNAME))
                .withTrustStore(tableOptions.get(SECURITY_TRUST_STORE));

        Optional<String> authType = tableOptions.getOptional(SECURITY_AUTH_TYPE);
        Optional<String> authToken = tableOptions.getOptional(SECURITY_AUTH_TOKEN);
        if (authType.isPresent() && authToken.isPresent() && !isCredentialsLoadDynamic()) {
            pravegaConfig.withCredentials(new FlinkPravegaUtils.SimpleCredentials(authType.get(), authToken.get()));
        }

        return pravegaConfig;
    }
}
