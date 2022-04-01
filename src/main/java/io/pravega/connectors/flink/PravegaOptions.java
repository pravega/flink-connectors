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
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * Details about each configuration could be found at {@link ClientConfig}.
 */
public class PravegaOptions {
    public static final String CONTROLLER_URI = "controllerURI";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String TRUST_STORE = "trustStore";
    public static final String VALIDATE_HOST_NAME = "validateHostName";
    public static final String MAX_CONNECTION_PER_SEGMENT_STORE = "maxConnectionsPerSegmentStore";
    public static final String ENABLE_TLS_TO_CONTROLLER = "enableTlsToController";
    public static final String ENABLE_TLS_TO_SEGMENT_STORE = "enableTlsToSegmentStore";
    /**
     * Configures the default Pravega scope, to resolve unqualified stream names and to support reader groups.
     */
    public static final String DEFAULT_SCOPE = "defaultScope";

    public static Properties getPropertiesFromEnvironmentAndCommand(@Nullable ParameterTool params) {
        Properties pravegaClientConfig = new Properties();

        Properties properties = System.getProperties();
        Map<String, String> env = System.getenv();

        Stream
                .of(PravegaOptions.class.getFields())
                .filter(field -> field.getType().equals(String.class))
                .map(field -> {
                    try {
                        return (String) field.get(null);
                    } catch (IllegalAccessException e) {
                        // Should never happen.
                        return "";
                    }
                })
                .forEach(optionName -> {
                    if (params != null && params.has(optionName)) {
                        pravegaClientConfig.put(optionName, params.get(optionName));
                    }
                    if (properties != null && properties.containsKey(optionName)) {
                        pravegaClientConfig.put(optionName, properties.getProperty(optionName));
                    }
                    if (env != null && env.containsKey(optionName)) {
                        pravegaClientConfig.put(optionName, env.get(optionName));
                    }
                });

        return pravegaClientConfig;
    }

    public static ClientConfig buildClientConfigFromProperties(Properties pravegaClientConfig) {
        ClientConfig.ClientConfigBuilder builder = ClientConfig.builder();
        Preconditions.checkState(pravegaClientConfig.containsKey(PravegaOptions.CONTROLLER_URI), "Controller uri must be provided!");
        builder.controllerURI(URI.create(pravegaClientConfig.getProperty(PravegaOptions.CONTROLLER_URI)));
        Preconditions.checkState(
                (pravegaClientConfig.containsKey(PravegaOptions.USERNAME) && pravegaClientConfig.containsKey(PravegaOptions.PASSWORD)) ||
                        (!pravegaClientConfig.containsKey(PravegaOptions.USERNAME) && !pravegaClientConfig.containsKey(PravegaOptions.PASSWORD)),
                "Username and password must be provided together or not!"
        );
        if (pravegaClientConfig.containsKey(PravegaOptions.USERNAME) && pravegaClientConfig.containsKey(PravegaOptions.PASSWORD)) {
            builder.credentials(new DefaultCredentials(
                    pravegaClientConfig.getProperty(PravegaOptions.USERNAME),
                    pravegaClientConfig.getProperty(PravegaOptions.PASSWORD))
            );
        }
        if (pravegaClientConfig.containsKey(PravegaOptions.VALIDATE_HOST_NAME)) {
            builder.validateHostName(Boolean.parseBoolean(pravegaClientConfig.getProperty(PravegaOptions.VALIDATE_HOST_NAME)));
        }
        if (pravegaClientConfig.containsKey(PravegaOptions.TRUST_STORE)) {
            builder.trustStore(pravegaClientConfig.getProperty(PravegaOptions.TRUST_STORE));
        }
        if (pravegaClientConfig.containsKey(PravegaOptions.MAX_CONNECTION_PER_SEGMENT_STORE)) {
            builder.maxConnectionsPerSegmentStore(Integer.parseInt(pravegaClientConfig.getProperty(PravegaOptions.MAX_CONNECTION_PER_SEGMENT_STORE)));
        }
        if (pravegaClientConfig.containsKey(PravegaOptions.ENABLE_TLS_TO_CONTROLLER)) {
            builder.enableTlsToController(Boolean.parseBoolean(pravegaClientConfig.getProperty(PravegaOptions.ENABLE_TLS_TO_CONTROLLER)));
        }
        if (pravegaClientConfig.containsKey(PravegaOptions.ENABLE_TLS_TO_SEGMENT_STORE)) {
            builder.enableTlsToSegmentStore(Boolean.parseBoolean(pravegaClientConfig.getProperty(PravegaOptions.ENABLE_TLS_TO_SEGMENT_STORE)));
        }
        return builder.build();
    }
}
