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

import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static io.pravega.connectors.flink.utils.DockerImageVersions.PRAVEGA;

/**
 * {@link RuntimeProvider} implementation, use the TestContainers as the backend. We would
 * start a Pravega container by this provider.
 */
public class PravegaContainerProvider implements RuntimeProvider {
    private static final Logger LOG = LoggerFactory.getLogger(PravegaContainerProvider.class);
    private static final String SCOPE = RandomStringUtils.randomAlphabetic(20);

    /**
     * Create a Pravega container provider by a predefined version, in DockerImageVersions.
     */
    private final PravegaContainer container = new PravegaContainer(DockerImageName.parse(PRAVEGA));

    private PravegaRuntimeOperator operator = null;

    @Override
    public void startUp() {
        // Prepare Pravega Container.
        container.withClasspathResourceMapping(
                "pravega-standalone.conf",
                "/opt/pravega/conf/standalone-config.properties",
                BindMode.READ_ONLY);

        // Start the Pravega Container.
        container.start();
        container.followOutput(new Slf4jLogConsumer(LOG).withSeparateOutputStreams());
        String clientTrustStorePath = String.format("%s.crt", createTempFile());
        container.copyFileFromContainer("/opt/pravega/conf/ca-cert.crt", clientTrustStorePath);

        // Create the operator.
        String controllerUriPrefix = isTlsEnable() ? "tls://" : "tcp://";
        this.operator = new PravegaRuntimeOperator(SCOPE, String.format("%s%s", controllerUriPrefix, container.getControllerUri()), clientTrustStorePath);
        this.operator.initialize();
    }

    @Override
    public void tearDown() throws IllegalStateException {
        try {
            operator.close();
            this.operator = null;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        container.stop();
    }

    @Override
    public PravegaRuntimeOperator operator() {
        return operator;
    }

    private String createTempFile()  {
        try {
            Path tempPath = Files.createTempFile("test-", "");
            tempPath.toFile().deleteOnExit();
            return tempPath.toFile().getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException("fail to create temp file", e);
        }
    }

    private boolean isTlsEnable() {
        Properties props = new Properties();

        try (InputStream resourceStream = PravegaContainerProvider.class.getClassLoader().getResourceAsStream("pravega-standalone.conf")) {
            props.load(resourceStream);
        } catch (IOException e) {
            throw new RuntimeException("fail to read pravega config file", e);
        }
        String isTlsEnable = props.getProperty("singlenode.security.tls.enable");

        return Boolean.parseBoolean(isTlsEnable);
    }
}
