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

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static io.pravega.connectors.flink.utils.runtime.SchemaRegistryContainer.SCHEMA_REGISTRY_PORT;

/**
 * This container wraps Pravega running in standalone mode.
 */
public class PravegaContainer extends GenericContainer<PravegaContainer> {

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("pravega/pravega");
    private static final String DEFAULT_TAG = "0.12.0";
    private static final int CONTROLLER_PORT = 9090;
    private static final int SEGMENT_STORE_PORT = 12345;

    public PravegaContainer() {
        this(DEFAULT_IMAGE_NAME.withTag(DEFAULT_TAG));
    }

    public PravegaContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);

        dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);
        addFixedExposedPort(CONTROLLER_PORT, CONTROLLER_PORT);
        addFixedExposedPort(SEGMENT_STORE_PORT, SEGMENT_STORE_PORT);
        withStartupTimeout(Duration.ofSeconds(90));
        withCommand("standalone");
        waitingFor(Wait.forLogMessage(".* Pravega Sandbox is running locally now.*", 1));

        // expose port for the SR container that connects to the same network as Pravega container
        addFixedExposedPort(SCHEMA_REGISTRY_PORT, SCHEMA_REGISTRY_PORT);
    }

    public String getControllerUri() {
        return String.format("tcp://%s:%d", getHost(), CONTROLLER_PORT);
    }
}
