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

import io.pravega.connectors.flink.utils.DockerImageVersions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link SchemaRegistryContainerRuntime} implementation, use the TestContainers as the backend. We would
 * start a Pravega Schema Registry container by this provider.
 */
public class SchemaRegistryContainerRuntime implements SchemaRegistryRuntime {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryContainerRuntime.class);

    /**
     * Create a Schema Registry container provider by a predefined version, in DockerImageVersions. this constance {@link
     * DockerImageVersions#SCHEMA_REGISTRY} should be bumped after the new Schema Registry release.
     */
    private final SchemaRegistryContainer container =
            new SchemaRegistryContainer(DockerImageName.parse(DockerImageVersions.SCHEMA_REGISTRY));

    private final AtomicBoolean started = new AtomicBoolean(false);

    private SchemaRegistryRuntimeOperator operator;

    @Override
    public void startUp(PravegaRuntimeOperator pravegaRuntimeOperator) {
        boolean haveStartedBefore = started.compareAndSet(false, true);
        if (!haveStartedBefore) {
            LOG.warn("You have started the Schema Registry Container. We will skip this execution.");
            return;
        }

        container.withNetworkMode("container:" + pravegaRuntimeOperator.getContainerId());

        // Start the Schema Registry Container.
        container.start();
        // Append the output to this runtime logger. Used for local debug purpose.
        container.followOutput(new Slf4jLogConsumer(LOG).withSeparateOutputStreams());

        // Create the operator.
        this.operator = new SchemaRegistryRuntimeOperator(pravegaRuntimeOperator, container.getSchemaRegistryUri());
    }

    @Override
    public void tearDown() {
        try {
            if (operator != null) {
                operator.close();
                this.operator = null;
            }
            container.stop();
            started.compareAndSet(true, false);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public SchemaRegistryRuntimeOperator operator() {
        return checkNotNull(operator, "You should start this Schema Registry container first.");
    }
}
