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

package io.pravega.connectors.flink;

import io.pravega.client.ClientConfig;
import io.pravega.shared.security.auth.DefaultCredentials;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.description.Description;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * Details about each configuration could be found at {@link ClientConfig}.
 */
public final class PravegaOptions {
    public static final String CLIENT_PREFIX = "pravega.";

    public static final ConfigOption<String> DEFAULT_SCOPE =
            ConfigOptions.key(CLIENT_PREFIX + "defaultScope")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("Configures the default Pravega scope, to resolve unqualified stream names and to support reader groups.")
                                    .build());
    public static final ConfigOption<String> CONTROLLER_URI =
            ConfigOptions.key(CLIENT_PREFIX + "controllerURI")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("Service URL provider for Pravega service.")
                                    .build());
    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key(CLIENT_PREFIX + "username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("The username to access Pravega.")
                                    .build());
    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key(CLIENT_PREFIX + "password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("The password to access Pravega.")
                                    .build());
    public static final ConfigOption<String> TRUST_STORE =
            ConfigOptions.key(CLIENT_PREFIX + "trustStore")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("The password to access Pravega.")
                                    .build());
    public static final ConfigOption<Boolean> VALIDATE_HOST_NAME =
            ConfigOptions.key(CLIENT_PREFIX + "validateHostName")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("Path to an optional truststore. If this is null or empty, the default JVM trust store is used.")
                                    .linebreak()
                                    .text("This is currently expected to be a signing certificate for the certification authority.")
                                    .build());
    public static final ConfigOption<Integer> MAX_CONNECTION_PER_SEGMENT_STORE =
            ConfigOptions.key(CLIENT_PREFIX + "maxConnectionsPerSegmentStore")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("An optional property representing whether to enable TLS for client's communication with the Controller.")
                                    .build());
    public static final ConfigOption<Boolean> ENABLE_TLS_TO_CONTROLLER =
            ConfigOptions.key(CLIENT_PREFIX + "enableTlsToController")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("Maximum number of connections per Segment store to be used by connection pooling.")
                                    .build());
    public static final ConfigOption<Boolean> ENABLE_TLS_TO_SEGMENT_STORE =
            ConfigOptions.key(CLIENT_PREFIX + "enableTlsToSegmentStore")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("Maximum number of connections per Segment store to be used by connection pooling.")
                                    .build());

    private PravegaOptions() {
        // This is a constant class.
    }

    public static Configuration getPropertiesFromEnvironmentAndCommand(@Nullable ParameterTool params) {
        Configuration pravegaClientConfig = new Configuration();

        Properties properties = System.getProperties();
        Map<String, String> env = System.getenv();

        Stream
                .of(PravegaOptions.class.getFields())
                .filter(field -> field.getType().equals(ConfigOption.class))
                .map(field -> {
                    try {
                        return (ConfigOption) field.get(null);
                    } catch (IllegalAccessException e) {
                        // Should never happen.
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .forEach(option -> {
                    if (params != null && params.has(option.key())) {
                        pravegaClientConfig.set(option, params.get(option.key()));
                    }
                    if (properties != null && properties.containsKey(option.key())) {
                        pravegaClientConfig.set(option, properties.getProperty(option.key()));
                    }
                    if (env != null && env.containsKey(option.key())) {
                        pravegaClientConfig.set(option, env.get(option.key()));
                    }
                });

        return pravegaClientConfig;
    }

    public static ClientConfig buildClientConfigFromProperties(Configuration pravegaClientConfig) {
        ClientConfig.ClientConfigBuilder builder = ClientConfig.builder();
        builder.controllerURI(URI.create(pravegaClientConfig.get(PravegaOptions.CONTROLLER_URI)));
        if (pravegaClientConfig.getOptional(PravegaOptions.USERNAME).isPresent() &&
                pravegaClientConfig.getOptional(PravegaOptions.PASSWORD).isPresent()) {
            builder.credentials(new DefaultCredentials(
                    pravegaClientConfig.get(PravegaOptions.USERNAME),
                    pravegaClientConfig.get(PravegaOptions.PASSWORD))
            );
        }
        pravegaClientConfig.getOptional(PravegaOptions.VALIDATE_HOST_NAME).ifPresent(builder::validateHostName);
        pravegaClientConfig.getOptional(PravegaOptions.TRUST_STORE).ifPresent(builder::trustStore);
        pravegaClientConfig.getOptional(PravegaOptions.MAX_CONNECTION_PER_SEGMENT_STORE).ifPresent(builder::maxConnectionsPerSegmentStore);
        pravegaClientConfig.getOptional(PravegaOptions.ENABLE_TLS_TO_CONTROLLER).ifPresent(builder::enableTlsToController);
        pravegaClientConfig.getOptional(PravegaOptions.ENABLE_TLS_TO_SEGMENT_STORE).ifPresent(builder::enableTlsToSegmentStore);
        return builder.build();
    }
}
