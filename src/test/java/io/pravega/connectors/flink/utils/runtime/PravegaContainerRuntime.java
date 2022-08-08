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
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link PravegaContainerRuntime} implementation, use the TestContainers as the backend. We would
 * start a Pravega container by this provider.
 */
public class PravegaContainerRuntime implements PravegaRuntime {

    private static final Logger LOG = LoggerFactory.getLogger(PravegaContainerRuntime.class);
    private static final String SCOPE = RandomStringUtils.randomAlphabetic(20);

    /**
     * Create a Pravega container provider by a predefined version, in DockerImageVersions. this constance {@link
     * DockerImageVersions#PRAVEGA} should be bumped after the new Pravega release.
     */
    private final PravegaContainer container = new PravegaContainer(DockerImageName.parse(DockerImageVersions.PRAVEGA));

    private final AtomicBoolean started = new AtomicBoolean(false);

    private PravegaRuntimeOperator operator;

    @Override
    public void startUp() {
        boolean haveStartedBefore = started.compareAndSet(false, true);
        if (!haveStartedBefore) {
            LOG.warn("You have started the Pravega Container. We will skip this execution.");
            return;
        }

        // Start the Pravega Container.
        container.start();
        // Append the output to this runtime logger. Used for local debug purpose.
        container.followOutput(new Slf4jLogConsumer(LOG).withSeparateOutputStreams());

        // Create the operator.
        this.operator = new PravegaRuntimeOperator(SCOPE, container.getControllerUri(), container.getContainerId());
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
    public PravegaRuntimeOperator operator() {
        return checkNotNull(operator, "You should start this Pravega container first.");
    }
}
