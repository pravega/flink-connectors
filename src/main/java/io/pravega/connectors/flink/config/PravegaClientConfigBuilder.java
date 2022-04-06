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
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * A builder for building the Pravega {@link ClientConfig} instance.
 */
public final class PravegaClientConfigBuilder {
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

    public static Configuration getConfigFromEnvironmentAndCommand(@Nullable ParameterTool params) {
        Configuration pravegaClientConfig = new Configuration();

        Properties properties = System.getProperties();
        Map<String, String> env = System.getenv();

        Stream
                .of(PravegaClientConfig.class.getFields())
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
}
