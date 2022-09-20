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
import io.pravega.client.stream.Stream;
import io.pravega.shared.security.auth.Credentials;
import io.pravega.shared.security.auth.DefaultCredentials;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link PravegaConfig}.
 */
public class PravegaConfigTest {

    private static final Credentials TEST_CREDENTIALS = new DefaultCredentials("password", "username");

    /**
     * Tests {@code resolve()} which performs stream name resolution.
     */
    @Test
    public void testStreamResolve() {
        // test parsing logic
        PravegaConfig config = new PravegaConfig(properties(PravegaConfig.SCOPE_PARAM, "scope1"), Collections.emptyMap(), ParameterTool.fromMap(Collections.emptyMap()));
        assertThat(config.getDefaultScope()).isEqualTo("scope1");
        Stream expectedStream = Stream.of("scope1/stream1");
        assertThat(config.resolve("stream1")).isEqualTo(expectedStream);
        assertThat(config.resolve("scope1/stream1")).isEqualTo(expectedStream);
        assertThat(config.resolve("scope2/stream1")).isNotEqualTo(expectedStream);

        // test that no default scope is needed when using qualified stream names
        config = new PravegaConfig(new Properties(), Collections.emptyMap(), ParameterTool.fromMap(Collections.emptyMap()));
        assertThat(config.getDefaultScope()).isNull();
        assertThat(config.resolve("scope1/stream1")).isEqualTo(expectedStream);

        // test an application-level default scope
        config = new PravegaConfig(new Properties(), Collections.emptyMap(), ParameterTool.fromMap(Collections.emptyMap()))
                .withDefaultScope(expectedStream.getScope());
        assertThat(config.resolve("stream1")).isEqualTo(expectedStream);
    }

    @Test
    public void testStreamResolveWithoutDefaultScope() {
        PravegaConfig config = new PravegaConfig(new Properties(), Collections.emptyMap(), ParameterTool.fromMap(Collections.emptyMap()));
        assertThatThrownBy(() -> config.resolve("stream1"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testScopePriority() {
        PravegaConfig config;

        config = new PravegaConfig(new Properties(), Collections.emptyMap(), ParameterTool.fromMap(Collections.emptyMap()))
                .withDefaultScope("scope1");
        assertThat(config.getDefaultScope()).isEqualTo("scope1");

        config = new PravegaConfig(properties(PravegaConfig.SCOPE_PARAM, "scope2"), Collections.emptyMap(), ParameterTool.fromMap(Collections.emptyMap()))
                .withDefaultScope("scope1");
        assertThat(config.getDefaultScope()).isEqualTo("scope2");

        config = new PravegaConfig(new Properties(), Collections.emptyMap(), ParameterTool.fromMap(Collections.emptyMap()))
                .withScope("scope1");
        assertThat(config.getDefaultScope()).isEqualTo("scope1");

        config = new PravegaConfig(properties(PravegaConfig.SCOPE_PARAM, "scope2"), Collections.emptyMap(), ParameterTool.fromMap(Collections.emptyMap()))
                .withScope("scope1");
        assertThat(config.getDefaultScope()).isEqualTo("scope1");
    }

    @Test
    public void testParameterResolve() {
        Properties properties = properties(PravegaConfig.CONTROLLER_PARAM, "property1");
        Map<String, String> variables = variables(PravegaConfig.CONTROLLER_PARAM, "variable1");
        ParameterTool parameters = parameters(PravegaConfig.CONTROLLER_PARAM, "parameter1");

        assertThat(PravegaConfig.CONTROLLER_PARAM.resolve(parameters, properties, variables))
                .isEqualTo(Optional.of("parameter1"));
        assertThat(PravegaConfig.CONTROLLER_PARAM.resolve(null, properties, variables))
                .isEqualTo(Optional.of("property1"));
        assertThat(PravegaConfig.CONTROLLER_PARAM.resolve(null, null, variables))
                .isEqualTo(Optional.of("variable1"));
        assertThat(PravegaConfig.CONTROLLER_PARAM.resolve(null, null, null))
                .isEqualTo(Optional.empty());
    }

    @Test
    public void testGetClientConfig() {
        // default controller URI
        PravegaConfig config = new PravegaConfig(new Properties(), Collections.emptyMap(), ParameterTool.fromMap(Collections.emptyMap()));
        ClientConfig clientConfig = config.getClientConfig();
        assertThat(clientConfig.getControllerURI()).isEqualTo(URI.create("tcp://localhost:9090"));
        assertThat(clientConfig.isValidateHostName()).isTrue();

        // explicitly-configured controller URI
        config = new PravegaConfig(new Properties(), Collections.emptyMap(), ParameterTool.fromMap(Collections.emptyMap()))
                .withControllerURI(URI.create("tcp://localhost:9090"))
                .withCredentials(TEST_CREDENTIALS)
                .withHostnameValidation(false);

        clientConfig = config.getClientConfig();
        assertThat(clientConfig.getControllerURI()).isEqualTo(URI.create("tcp://localhost:9090"));
        assertThat(clientConfig.getCredentials()).isEqualTo(TEST_CREDENTIALS);
        assertThat(clientConfig.isValidateHostName()).isFalse();
    }

    // helpers

    private static Properties properties(PravegaConfig.PravegaParameter parameter, String value) {
        Properties properties = new Properties();
        properties.setProperty(parameter.getPropertyName(), value);
        return properties;
    }

    private static Map<String, String> variables(PravegaConfig.PravegaParameter parameter, String value) {
        return Collections.singletonMap(parameter.getVariableName(), value);
    }

    private static ParameterTool parameters(PravegaConfig.PravegaParameter parameter, String value) {
        return ParameterTool.fromMap(Collections.singletonMap(parameter.getParameterName(), value));
    }
}