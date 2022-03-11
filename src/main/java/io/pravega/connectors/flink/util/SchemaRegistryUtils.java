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
package io.pravega.connectors.flink.util;

import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.shared.credentials.PravegaCredentialProvider;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.pravega.connectors.flink.util.FlinkPravegaUtils.isCredentialsLoadDynamic;

public class SchemaRegistryUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryUtils.class);

    /**
     * Gets the schema registry client.
     *
     * @param pravegaConfig Pravega configuration
     * @return the configuration for schema registry client
     */
    public static SchemaRegistryClientConfig getSchemaRegistryClientConfig(PravegaConfig pravegaConfig) {
        Preconditions.checkNotNull(pravegaConfig.getDefaultScope(), "Default Scope should be set for schema registry client");
        Preconditions.checkNotNull(pravegaConfig.getSchemaRegistryUri(), "Schema Registry URI should be set for schema registry client");

        SchemaRegistryClientConfig.SchemaRegistryClientConfigBuilder builder = SchemaRegistryClientConfig.builder()
                .schemaRegistryUri(pravegaConfig.getSchemaRegistryUri());

        if (pravegaConfig.getCredentials() != null) {
            // basic credential
            builder.authentication(
                    pravegaConfig.getCredentials().getAuthenticationType(),
                    pravegaConfig.getCredentials().getAuthenticationToken()
            );
        } else {
            if (isCredentialsLoadDynamic()) {
                // dynamic credential, e.g. Keycloak
                builder.authentication(new PravegaCredentialProvider(pravegaConfig.getClientConfig()));
            } else {
                // no credential
            }
        }

        return builder.build();
    }

    /**
     * Get the serialization format for the given group under which the schemas are registered.
     *
     * @param pravegaConfig Pravega configuration
     * @param group the group under which the schemas are registered
     * @return the serialization format
     */
    public static SerializationFormat getSerializationFormat(PravegaConfig pravegaConfig, String group) {
        try (SchemaRegistryClient schemaRegistryClient = getSchemaRegistryClient(pravegaConfig)) {
            return schemaRegistryClient.getGroupProperties(group).getSerializationFormat();
        } catch (Exception e) {
            LOG.error("Error while closing the schema registry client", e);
            throw new FlinkRuntimeException(e);
        }
    }

    /**
     * Get the schema info for the given group under which the schemas are registered.
     *
     * @param pravegaConfig Pravega configuration
     * @param group the group under which the schemas are registered
     * @return the schema info
     */
    public static SchemaInfo getSchemaInfo(PravegaConfig pravegaConfig, String group) {
        try (SchemaRegistryClient schemaRegistryClient = getSchemaRegistryClient(pravegaConfig)) {
            return schemaRegistryClient.getSchemas(group).get(0).getSchemaInfo();
        } catch (Exception e) {
            LOG.error("Error while closing the schema registry client", e);
            throw new FlinkRuntimeException(e);
        }
    }

    /**
     * Get the serializer config for the given namespace and group.
     *
     * @param namespace the namespace
     * @param group the group under which the schemas are registered
     * @param pravegaConfig Pravega configuration
     * @return the serializer config
     */
    public static SerializerConfig getSerializerConfig(String namespace, String group, PravegaConfig pravegaConfig) {
        return SerializerConfig.builder()
                .namespace(namespace)
                .groupId(group)
                .registerSchema(false)
                .registryConfig(getSchemaRegistryClientConfig(pravegaConfig))
                .build();
    }

    private static SchemaRegistryClient getSchemaRegistryClient(PravegaConfig pravegaConfig) {
        return SchemaRegistryClientFactory.withNamespace(
                pravegaConfig.getDefaultScope(), getSchemaRegistryClientConfig(pravegaConfig));
    }
}
