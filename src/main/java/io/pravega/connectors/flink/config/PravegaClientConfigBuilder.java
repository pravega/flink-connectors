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
import io.pravega.shared.security.auth.DefaultCredentials;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * Helper methods for {@link PravegaClientConfig}.
 */
public final class PravegaClientConfigBuilder {
    /**
     * A builder for building the Pravega {@link ClientConfig} instance.
     *
     * @param pravegaClientConfig The configuration.
     * @return A Pravega {@link ClientConfig} instance.
     */
    public static ClientConfig buildClientConfigFromProperties(Configuration pravegaClientConfig) {
        ClientConfig.ClientConfigBuilder builder = ClientConfig.builder();
        builder.controllerURI(URI.create(pravegaClientConfig.get(PravegaClientConfig.CONTROLLER_URI)));
        if (pravegaClientConfig.getOptional(PravegaClientConfig.USERNAME).isPresent() &&
                pravegaClientConfig.getOptional(PravegaClientConfig.PASSWORD).isPresent()) {
            builder.credentials(new DefaultCredentials(
                    pravegaClientConfig.get(PravegaClientConfig.USERNAME),
                    pravegaClientConfig.get(PravegaClientConfig.PASSWORD))
            );
        }
        pravegaClientConfig.getOptional(PravegaClientConfig.VALIDATE_HOST_NAME).ifPresent(builder::validateHostName);
        pravegaClientConfig.getOptional(PravegaClientConfig.TRUST_STORE).ifPresent(builder::trustStore);
        pravegaClientConfig.getOptional(PravegaClientConfig.MAX_CONNECTION_PER_SEGMENT_STORE).ifPresent(builder::maxConnectionsPerSegmentStore);
        pravegaClientConfig.getOptional(PravegaClientConfig.ENABLE_TLS_TO_CONTROLLER).ifPresent(builder::enableTlsToController);
        pravegaClientConfig.getOptional(PravegaClientConfig.ENABLE_TLS_TO_SEGMENT_STORE).ifPresent(builder::enableTlsToSegmentStore);
        return builder.build();
    }

    /**
     * Get configuration from command line and system environment.
     *
     * @param params Command line params from {@link ParameterTool#fromArgs(String[])}
     * @return A Pravega client configuration.
     */
    public static Configuration getConfigFromEnvironmentAndCommand(@Nullable ParameterTool params) {
        Configuration pravegaClientConfig = new Configuration();

        Properties properties = System.getProperties();
        Map<String, String> env = System.getenv();

        final class EnvOption {
            final String parameterName;
            final String propertyName;
            final String variableName;
            final ConfigOption<String> configOption;

            EnvOption(String parameterName, String propertyName, String variableName, ConfigOption<String> configOption) {
                this.parameterName = parameterName;
                this.propertyName = propertyName;
                this.variableName = variableName;
                this.configOption = configOption;
            }
        }

        Stream
                .of(
                        new EnvOption("controller", "pravega.controller.uri", "PRAVEGA_CONTROLLER_URI", PravegaClientConfig.CONTROLLER_URI),
                        new EnvOption("scope", "pravega.scope", "PRAVEGA_SCOPE", PravegaClientConfig.DEFAULT_SCOPE),
                        new EnvOption("schema-registry", "pravega.schema-registry.uri", "PRAVEGA_SCHEMA_REGISTRY_URI", PravegaClientConfig.SCHEMA_REGISTRY_URI)
                )
                .forEach(option -> {
                    if (params != null && params.has(option.parameterName)) {
                        pravegaClientConfig.set(option.configOption, params.get(option.parameterName));
                    }
                    if (properties != null && properties.containsKey(option.propertyName)) {
                        pravegaClientConfig.set(option.configOption, properties.getProperty(option.propertyName));
                    }
                    if (env != null && env.containsKey(option.variableName)) {
                        pravegaClientConfig.set(option.configOption, env.get(option.variableName));
                    }
                });

        return pravegaClientConfig;
    }
}
