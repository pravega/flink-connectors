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

package io.pravega.connectors.flink.utils.runtime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;

import static io.pravega.connectors.flink.utils.DockerImageVersions.SCHEMA_REGISTRY;

/**
 * {@link SchemaRegistryRuntimeProvider} implementation, use the TestContainers as the backend. We would
 * start a Pravega Schema Registry container by this provider.
 */
public class SchemaRegistryContainerProvider implements SchemaRegistryRuntimeProvider {
    private static final Logger LOG = LoggerFactory.getLogger(PravegaContainerProvider.class);

    private SchemaRegistryContainer container;

    private SchemaRegistryRuntimeOperator operator = null;

    @Override
    public void startUp(PravegaRuntimeOperator pravegaRuntimeOperator) {
        container = new SchemaRegistryContainer(
                DockerImageName.parse(SCHEMA_REGISTRY), pravegaRuntimeOperator.getContainerId());
        container.start();
        container.followOutput(new Slf4jLogConsumer(LOG).withSeparateOutputStreams());

        this.operator = new SchemaRegistryRuntimeOperator(pravegaRuntimeOperator, container.getSchemaRegistryUri());
        this.operator.initialize();
    }

    @Override
    public void tearDown() {
        try {
            operator.close();
            this.operator = null;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        container.stop();
    }

    @Override
    public SchemaRegistryRuntimeOperator operator() {
        return operator;
    }
}
