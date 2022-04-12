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
import org.apache.flink.configuration.Configuration;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.Map;
import java.util.Properties;

import static io.pravega.connectors.flink.util.FlinkPravegaUtils.isCredentialsLoadDynamic;

/**
 * Helper methods for {@link PravegaClientConfig}.
 */
public final class PravegaClientConfigUtils {
    /**
     * A builder for building the Pravega {@link ClientConfig} instance.
     *
     * @param pravegaClientConfig The configuration.
     * @return A Pravega {@link ClientConfig} instance.
     */
    public static ClientConfig buildClientConfigFromProperties(Configuration pravegaClientConfig) {
        ClientConfig.ClientConfigBuilder builder = ClientConfig.builder();
        builder.controllerURI(URI.create(pravegaClientConfig.get(PravegaClientConfig.CONTROLLER_URI)));
        if (!isCredentialsLoadDynamic() &&
                pravegaClientConfig.getOptional(PravegaClientConfig.USERNAME).isPresent() &&
                pravegaClientConfig.getOptional(PravegaClientConfig.PASSWORD).isPresent()) {
            builder.credentials(new DefaultCredentials(
                    pravegaClientConfig.get(PravegaClientConfig.PASSWORD),
                    pravegaClientConfig.get(PravegaClientConfig.USERNAME))
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

        if (params != null && params.has("controller")) {
            pravegaClientConfig.set(PravegaClientConfig.CONTROLLER_URI, params.get("controller"));
        }
        if (properties != null && properties.containsKey("pravega.controller.uri")) {
            pravegaClientConfig.set(PravegaClientConfig.CONTROLLER_URI, properties.getProperty("pravega.controller.uri"));
        }
        if (env != null && env.containsKey("PRAVEGA_CONTROLLER_URI")) {
            pravegaClientConfig.set(PravegaClientConfig.CONTROLLER_URI, env.get("PRAVEGA_CONTROLLER_URI"));
        }

        if (params != null && params.has("scope")) {
            pravegaClientConfig.set(PravegaClientConfig.DEFAULT_SCOPE, params.get("scope"));
        }
        if (properties != null && properties.containsKey("pravega.scope")) {
            pravegaClientConfig.set(PravegaClientConfig.DEFAULT_SCOPE, properties.getProperty("pravega.scope"));
        }
        if (env != null && env.containsKey("PRAVEGA_SCOPE")) {
            pravegaClientConfig.set( PravegaClientConfig.DEFAULT_SCOPE, env.get("PRAVEGA_CONTROLLER_URI"));
        }

        return pravegaClientConfig;
    }
}
