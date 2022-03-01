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

package io.pravega.connectors.flink.serialization;

import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.util.SchemaRegistryUtils;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public abstract class AbstractSerializerFromSchemaRegistry implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractSerializerFromSchemaRegistry.class);

    private static final long serialVersionUID = 1L;

    protected SerializationFormat format;
    protected SchemaInfo schemaInfo;
    protected SerializerConfig serializerConfig;

    private final PravegaConfig pravegaConfig;
    private final String group;

    protected AbstractSerializerFromSchemaRegistry(PravegaConfig pravegaConfig, String group) {
        Preconditions.checkNotNull(pravegaConfig.getSchemaRegistryUri());
        this.pravegaConfig = pravegaConfig;
        this.group = group;
    }

    // initialize the (de)serializer
    protected abstract void initialize();

    // connect to Schema Registry service
    protected void open() {
        synchronized (this) {
            SchemaRegistryClientConfig schemaRegistryClientConfig = SchemaRegistryUtils.getSchemaRegistryClientConfig(pravegaConfig);

            try (SchemaRegistryClient schemaRegistryClient = SchemaRegistryClientFactory.withNamespace(
                    pravegaConfig.getDefaultScope(), schemaRegistryClientConfig)) {
                format = schemaRegistryClient.getGroupProperties(group).getSerializationFormat();
                schemaInfo = schemaRegistryClient.getSchemas(group).get(0).getSchemaInfo();
            } catch (Exception e) {
                LOG.error("Error while closing the schema registry client", e);
                throw new FlinkRuntimeException(e);
            }

            serializerConfig = SerializerConfig.builder()
                    .namespace(pravegaConfig.getDefaultScope())
                    .groupId(group)
                    .registerSchema(false)
                    .registryConfig(schemaRegistryClientConfig)
                    .build();
        }
    }
}
