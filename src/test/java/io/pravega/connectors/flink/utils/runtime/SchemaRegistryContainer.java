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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

/**
 * This container wraps Pravega Schema Registry running in standalone mode.
 */
public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("pravega/schemaregistry");
    private static final int PORT = 9092;

    public SchemaRegistryContainer(final DockerImageName dockerImageName, String pravegaContainerId) {
        super(dockerImageName);

        dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);
        withNetworkMode("container:" + pravegaContainerId);
        withStartupTimeout(Duration.ofSeconds(90));
        waitingFor(Wait.forLogMessage(".* Starting REST server listening on port.*", 1));
    }

    public String getSchemaRegistryUri() {
        return String.format("http://%s:%d", getHost(), PORT);
    }
}
