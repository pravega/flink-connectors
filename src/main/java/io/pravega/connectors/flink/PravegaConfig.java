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
import lombok.Data;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * The Pravega client configuration.
 */
public class PravegaConfig implements Serializable {

    static final PravegaParameter CONTROLLER_PARAM = new PravegaParameter("controller", "pravega.controller.uri", "PRAVEGA_CONTROLLER_URI");
    static final PravegaParameter SCOPE_PARAM = new PravegaParameter("scope", "pravega.scope", "PRAVEGA_SCOPE");

    private static final long serialVersionUID = 1L;

    private URI controllerURI;
    private String defaultScope;
    private Credentials credentials;
    private boolean validateHostname = true;
    private String trustStore;

    // region Factory methods
    PravegaConfig(Properties properties, Map<String, String> env, ParameterTool params) {
        this.controllerURI = CONTROLLER_PARAM.resolve(params, properties, env).map(URI::create).orElse(null);
        this.defaultScope = SCOPE_PARAM.resolve(params, properties, env).orElse(null);
    }

    /**
     * Gets a configuration based on defaults obtained from the local environment.
     */
    public static PravegaConfig fromDefaults() {
        return new PravegaConfig(System.getProperties(), System.getenv(), ParameterTool.fromMap(Collections.emptyMap()));
    }

    /**
     * Gets a configuration based on defaults obtained from the local environment plus the given program parameters.
     *
     * @param params the parameters to use.
     */
    public static PravegaConfig fromParams(ParameterTool params) {
        return new PravegaConfig(System.getProperties(), System.getenv(), params);
    }

    // endregion

    /**
     * Gets the {@link ClientConfig} to use with the Pravega client.
     */
    public ClientConfig getClientConfig() {
        ClientConfig.ClientConfigBuilder builder = ClientConfig.builder()
                .validateHostName(validateHostname);
        if (controllerURI != null) {
            builder.controllerURI(controllerURI);
        }
        if (credentials != null) {
            builder.credentials(credentials);
        }
        if (trustStore != null) {
            builder.trustStore(trustStore);
        }
        return builder.build();
    }

    /**
     * Resolves the given stream name.
     *
     * The scope name is resolved in the following order:
     * 1. from the stream name (if fully-qualified)
     * 2. from the program argument {@code --scope} (if program arguments were provided to the {@link PravegaConfig})
     * 3. from the system property {@code pravega.scope}
     * 4. from the system environment variable {@code PRAVEGA_SCOPE}
     *
     * @param streamSpec a qualified or unqualified stream name
     * @return a fully-qualified stream name
     * @throws IllegalStateException if an unqualified stream name is supplied but the scope is not configured.
     */
    public Stream resolve(String streamSpec) {
        Preconditions.checkNotNull(streamSpec, "streamSpec");
        String[] split = streamSpec.split("/", 2);
        if (split.length == 1) {
            // unqualified
            Preconditions.checkState(defaultScope != null, "The default scope is not configured.");
            return Stream.of(defaultScope, split[0]);
        } else {
            // qualified
            assert split.length == 2;
            return Stream.of(split[0], split[1]);
        }
    }

    // region Discovery

    /**
     * Configures the Pravega controller RPC URI.
     *
     * @param controllerURI The URI.
     */
    public PravegaConfig withControllerURI(URI controllerURI) {
        this.controllerURI = controllerURI;
        return this;
    }

    /**
     * Configures truststore value.
     * @param trustStore truststore name.
     * @return current instance of PravegaConfig.
     */
    public PravegaConfig withTrustStore(String trustStore) {
        this.trustStore = trustStore;
        return this;
    }

    /**
     * Configures the default Pravega scope, to resolve unqualified stream names and to support reader groups.
     *
     * @param scope The scope to use (with lowest priority).
     */
    public PravegaConfig withDefaultScope(String scope) {
        if (this.defaultScope == null) {
            this.defaultScope = scope;
        }
        return this;
    }

    /**
     * Gets the default Pravega scope.
     */
    @Nullable
    public String getDefaultScope() {
        return defaultScope;
    }

    // endregion

    // region Security

    /**
     * Configures the Pravega credentials to use.
     *
     * @param credentials a credentials object.
     */
    public PravegaConfig withCredentials(Credentials credentials) {
        this.credentials = credentials;
        return this;
    }

    /**
     * Enables or disables TLS hostname validation (default: true).
     *
     * @param validateHostname a boolean indicating whether to validate the hostname on incoming requests.
     */
    public PravegaConfig withHostnameValidation(boolean validateHostname) {
        this.validateHostname = validateHostname;
        return this;
    }

    // endregion

    /**
     * A configuration parameter resolvable via command-line parameters, system properties, or OS environment variables.
     */
    @Data
    static class PravegaParameter implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String parameterName;
        private final String propertyName;
        private final String variableName;

        public Optional<String> resolve(ParameterTool parameters, Properties properties, Map<String, String> variables) {
            if (parameters != null && parameters.has(parameterName)) {
                return Optional.of(parameters.get(parameterName));
            }
            if (properties != null && properties.containsKey(propertyName)) {
                return Optional.of(properties.getProperty(propertyName));
            }
            if (variables != null && variables.containsKey(variableName)) {
                return Optional.of(variables.get(variableName));
            }
            return Optional.empty();
        }
    }
}
