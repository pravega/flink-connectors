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
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.serializer.shared.credentials.PravegaCredentialProvider;
import org.apache.flink.util.Preconditions;

import static io.pravega.connectors.flink.util.FlinkPravegaUtils.isCredentialsLoadDynamic;

public class SchemaRegistryUtils {

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
                builder.authentication(new PravegaCredentialProvider());
            } else {
                // no credential
            }
        }

        return builder.build();
    }
}
