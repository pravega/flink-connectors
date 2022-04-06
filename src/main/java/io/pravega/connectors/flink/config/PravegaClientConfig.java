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

package io.pravega.connectors.flink.config;

import io.pravega.client.ClientConfig;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;

/**
 * Details about each configuration could be found at {@link ClientConfig}.
 */
public final class PravegaClientConfig {
    public static final String CLIENT_PREFIX = "pravega.";
    public static final String CLIENT_SECURITY_PREFIX = "security.";

    public static final ConfigOption<String> DEFAULT_SCOPE =
            ConfigOptions.key(CLIENT_PREFIX + "defaultScope")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Configures the default Pravega scope, to resolve unqualified stream names and to support reader groups.");
    public static final ConfigOption<String> CONTROLLER_URI =
            ConfigOptions.key(CLIENT_PREFIX + "controllerURI")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Service URL provider for Pravega service.");
    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key(CLIENT_PREFIX + CLIENT_SECURITY_PREFIX + "username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The username to access Pravega.");
    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key(CLIENT_PREFIX + CLIENT_SECURITY_PREFIX + "password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The password to access Pravega.");
    public static final ConfigOption<String> TRUST_STORE =
            ConfigOptions.key(CLIENT_PREFIX + CLIENT_SECURITY_PREFIX + "trustStore")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("Path to an optional truststore. If this is null or empty, the default JVM trust store is used.")
                                    .linebreak()
                                    .text("This is currently expected to be a signing certificate for the certification authority.")
                                    .build());
    public static final ConfigOption<Boolean> VALIDATE_HOST_NAME =
            ConfigOptions.key(CLIENT_PREFIX + CLIENT_SECURITY_PREFIX + "validateHostName")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("Whether to enable host name validation or not.");
    public static final ConfigOption<Integer> MAX_CONNECTION_PER_SEGMENT_STORE =
            ConfigOptions.key(CLIENT_PREFIX + "maxConnectionsPerSegmentStore")
                    .intType()
                    .noDefaultValue()
                    .withDescription("An optional property representing whether to enable TLS for client's communication with the Controller.");
    public static final ConfigOption<Boolean> ENABLE_TLS_TO_CONTROLLER =
            ConfigOptions.key(CLIENT_PREFIX + CLIENT_SECURITY_PREFIX + "enableTlsToController")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("Maximum number of connections per Segment store to be used by connection pooling.");
    public static final ConfigOption<Boolean> ENABLE_TLS_TO_SEGMENT_STORE =
            ConfigOptions.key(CLIENT_PREFIX + CLIENT_SECURITY_PREFIX + "enableTlsToSegmentStore")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("Maximum number of connections per Segment store to be used by connection pooling.");
    public static final ConfigOption<String> SCHEMA_REGISTRY_URI =
            ConfigOptions.key(CLIENT_PREFIX + "schemaRegistryURI")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Configures the Pravega schema registry URI.");

    private PravegaClientConfig() {
        // This is a constant class.
    }
}
