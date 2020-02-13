/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink;

import io.pravega.client.ClientConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.Credentials;
import io.pravega.client.stream.impl.DefaultCredentials;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.Test;

import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
        assertEquals("scope1", config.getDefaultScope());
        Stream expectedStream = Stream.of("scope1/stream1");
        assertEquals(expectedStream, config.resolve("stream1"));
        assertEquals(expectedStream, config.resolve("scope1/stream1"));
        assertNotEquals(expectedStream, config.resolve("scope2/stream1"));

        // test that no default scope is needed when using qualified stream names
        config = new PravegaConfig(new Properties(), Collections.emptyMap(), ParameterTool.fromMap(Collections.emptyMap()));
        assertNull(config.getDefaultScope());
        assertEquals(expectedStream, config.resolve("scope1/stream1"));

        // test an application-level default scope
        config = new PravegaConfig(new Properties(), Collections.emptyMap(), ParameterTool.fromMap(Collections.emptyMap()))
                .withDefaultScope(expectedStream.getScope());
        assertEquals(expectedStream, config.resolve("stream1"));
    }

    @Test(expected = IllegalStateException.class)
    public void testStreamResolveWithoutDefaultScope() {
        PravegaConfig config = new PravegaConfig(new Properties(), Collections.emptyMap(), ParameterTool.fromMap(Collections.emptyMap()));
        config.resolve("stream1");
    }

    @Test
    public void testScopePriority() {
        PravegaConfig config;

        config = new PravegaConfig(new Properties(), Collections.emptyMap(), ParameterTool.fromMap(Collections.emptyMap()))
                .withDefaultScope("scope1");
        assertEquals("scope1", config.getDefaultScope());

        config = new PravegaConfig(properties(PravegaConfig.SCOPE_PARAM, "scope2"), Collections.emptyMap(), ParameterTool.fromMap(Collections.emptyMap()))
                .withDefaultScope("scope1");
        assertEquals("scope2", config.getDefaultScope());
    }

    @Test
    public void testParameterResolve() {
        Properties properties = properties(PravegaConfig.CONTROLLER_PARAM, "property1");
        Map<String, String> variables = variables(PravegaConfig.CONTROLLER_PARAM, "variable1");
        ParameterTool parameters = parameters(PravegaConfig.CONTROLLER_PARAM, "parameter1");

        assertEquals(Optional.of("parameter1"), PravegaConfig.CONTROLLER_PARAM.resolve(parameters, properties, variables));
        assertEquals(Optional.of("property1"), PravegaConfig.CONTROLLER_PARAM.resolve(null, properties, variables));
        assertEquals(Optional.of("variable1"), PravegaConfig.CONTROLLER_PARAM.resolve(null, null, variables));
        assertEquals(Optional.empty(), PravegaConfig.CONTROLLER_PARAM.resolve(null, null, null));
    }

    @Test
    public void testGetClientConfig() {
        // default controller URI
        PravegaConfig config = new PravegaConfig(new Properties(), Collections.emptyMap(), ParameterTool.fromMap(Collections.emptyMap()));
        ClientConfig clientConfig = config.getClientConfig();
        assertEquals(URI.create("tcp://localhost:9090"), clientConfig.getControllerURI());
        assertTrue(clientConfig.isValidateHostName());

        // explicitly-configured controller URI
        config = new PravegaConfig(new Properties(), Collections.emptyMap(), ParameterTool.fromMap(Collections.emptyMap()))
                .withControllerURI(URI.create("tcp://localhost:9090"))
                .withCredentials(TEST_CREDENTIALS)
                .withHostnameValidation(false);

        clientConfig = config.getClientConfig();
        assertEquals(URI.create("tcp://localhost:9090"), clientConfig.getControllerURI());
        assertEquals(TEST_CREDENTIALS, clientConfig.getCredentials());
        assertFalse(clientConfig.isValidateHostName());
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