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
package io.pravega.connectors.flink.utils;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.PravegaOptions;
import io.pravega.local.InProcPravegaCluster;
import io.pravega.shared.security.auth.DefaultCredentials;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Utility functions for creating the test setup.
 */
@NotThreadSafe
public final class SetupUtils {
    private static final Logger LOG = LoggerFactory.getLogger(SetupUtils.class);

    private static final String PRAVEGA_USERNAME = "admin";
    private static final String PRAVEGA_PASSWORD = "1111_aaaa";
    private static final String PASSWD_FILE = "passwd";
    private static final String KEY_FILE = "server-key.key";
    private static final String CERT_FILE = "server-cert.crt";
    private static final String CLIENT_TRUST_STORE_FILE = "ca-cert.crt";
    private static final String STANDALONE_KEYSTORE_FILE = "server.keystore.jks";
    private static final String STANDALONE_TRUSTSTORE_FILE = "client.truststore.jks";
    private static final String STANDALONE_KEYSTORE_PASSWD_FILE = "server.keystore.jks.passwd";

    private final PravegaGateway gateway;

    // Manage the state of the class.
    private final AtomicBoolean started = new AtomicBoolean(false);

    // auth enabled by default. Set it to false to disable Pravega authentication and authorization.
    private boolean enableAuth = true;

    // Set to true to enable TLS
    private boolean enableTls = true;

    private boolean enableHostNameValidation = false;

    private boolean enableRestServer = true;

    private EventStreamClientFactory eventStreamClientFactory;

    // The test Scope name.
    private final String scope = RandomStringUtils.randomAlphabetic(20);

    public SetupUtils() {
        this(System.getProperty("pravega.uri"));
    }

    public SetupUtils(String externalUri) {
        if (externalUri != null) {
            LOG.info("Using Pravega services at {}.", externalUri);
            gateway = new ExternalPravegaGateway(URI.create(externalUri));
        } else {
            LOG.info("Starting in-process Pravega services.");
            gateway = new InProcPravegaGateway();
        }
    }

    public void setEnableAuth(boolean enableAuth) {
        this.enableAuth = enableAuth;
    }

    public void setEnableTls(boolean enableTls) {
        this.enableTls = enableTls;
    }

    public void setEnableHostNameValidation(boolean enableHostNameValidation) {
        this.enableHostNameValidation = enableHostNameValidation;
    }

    public boolean isEnableHostNameValidation() {
        return enableHostNameValidation;
    }

    public String getScope() {
        return scope;
    }

    /**
     * Start all pravega related services required for the test deployment.
     *
     * @throws Exception on any errors.
     */
    public void startAllServices() throws Exception {
        if (!this.started.compareAndSet(false, true)) {
            LOG.warn("Services already started, not attempting to start again");
            return;
        }
        gateway.start();
        eventStreamClientFactory = EventStreamClientFactory.withScope(this.scope, getClientConfig());
    }

    /**
     * Stop the pravega cluster and release all resources.
     *
     * @throws Exception on any errors.
     */
    public void stopAllServices() throws Exception {
        if (!this.started.compareAndSet(true, false)) {
            LOG.warn("Services not yet started or already stopped, not attempting to stop");
            return;
        }

        if (eventStreamClientFactory != null) {
            eventStreamClientFactory.close();
        }

        try {
            gateway.stop();
        } catch (Exception e) {
            LOG.warn("Services did not stop cleanly (" + e.getMessage() + ")", e);
        }
    }

    /**
     * Get resources path from resource
     *
     * @param resourceName    Name of the resource.
     *
     * @return Path of the resource file.
     */
    static String getPathFromResource(String resourceName) {
        return SetupUtils.class.getClassLoader().getResource(resourceName).getPath();
    }


    /**
     * Fetch the controller endpoint for this cluster.
     *
     * @return URI The controller endpoint to connect to this cluster.
     */
    public URI getControllerUri() {
        return getClientConfig().getControllerURI();
    }

    /**
     * Fetch the client configuration with which to connect to the controller.
     */
    public ClientConfig getClientConfig() {
        return this.gateway.getClientConfig();
    }

    /**
     * Fetch the auth type.
     */
    public String getAuthType() {
        return "Basic";
    }

    /**
     * Fetch the auth token.
     */
    public String getAuthToken() {
        String decoded = PRAVEGA_USERNAME + ":" + PRAVEGA_PASSWORD;
        return Base64.getEncoder().encodeToString(decoded.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Fetch Pravega client trust store.
     */
    public String getPravegaClientTrustStore() {
        return getPathFromResource(CLIENT_TRUST_STORE_FILE);
    }

    /**
     * Fetch the {@link PravegaConfig} for integration test purposes.
     */
    public PravegaConfig getPravegaConfig() {
        return PravegaConfig.fromDefaults()
                .withControllerURI(getControllerUri())
                .withDefaultScope(getScope())
                .withCredentials(new DefaultCredentials(PRAVEGA_PASSWORD, PRAVEGA_USERNAME))
                .withHostnameValidation(enableHostNameValidation)
                .withTrustStore(getPathFromResource(CLIENT_TRUST_STORE_FILE));
    }

    public Configuration getPravegaClientConfig() {
        final Configuration pravegaClientConfig = new Configuration();
        pravegaClientConfig.set(PravegaOptions.CONTROLLER_URI, getControllerUri().toString());
        pravegaClientConfig.set(PravegaOptions.DEFAULT_SCOPE, getScope());
        pravegaClientConfig.set(PravegaOptions.USERNAME, PRAVEGA_PASSWORD);
        pravegaClientConfig.set(PravegaOptions.PASSWORD, PRAVEGA_USERNAME);
        pravegaClientConfig.set(PravegaOptions.VALIDATE_HOST_NAME, enableHostNameValidation);
        pravegaClientConfig.set(PravegaOptions.TRUST_STORE, getPathFromResource(CLIENT_TRUST_STORE_FILE));
        return pravegaClientConfig;
    }

    /**
     * Create the test stream.
     *
     * @param streamName     Name of the test stream.
     * @param numSegments    Number of segments to be created for this stream.
     *
     * @throws Exception on any errors.
     */
    public void createTestStream(final String streamName, final int numSegments)
            throws Exception {
        Preconditions.checkState(this.started.get(), "Services not yet started");
        Preconditions.checkNotNull(streamName);
        Preconditions.checkArgument(numSegments > 0);

        try (StreamManager streamManager = StreamManager.create(getClientConfig())) {
            streamManager.createScope(this.scope);
            streamManager.createStream(this.scope, streamName,
                    StreamConfiguration.builder()
                            .scalingPolicy(ScalingPolicy.fixed(numSegments))
                            .build());
            LOG.info("Created stream: " + streamName);
        }
    }

    /**
     * Get the stream.
     *
     * @param streamName     Name of the test stream.
     *
     * @return a Stream
     */
    public Stream getStream(final String streamName) {
        return Stream.of(this.scope, streamName);
    }

    /**
     * Create a stream writer for writing Integer events.
     *
     * @param streamName    Name of the test stream.
     *
     * @return Stream writer instance.
     */
    public EventStreamWriter<Integer> getIntegerWriter(final String streamName) {
        Preconditions.checkState(this.started.get(), "Services not yet started");
        Preconditions.checkNotNull(streamName);

        return eventStreamClientFactory.createEventWriter(
                streamName,
                new IntegerSerializer(),
                EventWriterConfig.builder().build());
    }

    /**
     * Create a stream reader for reading Integer events.
     *
     * @param streamName    Name of the test stream.
     *
     * @return Stream reader instance.
     */
    public EventStreamReader<Integer> getIntegerReader(final String streamName) {
        Preconditions.checkState(this.started.get(), "Services not yet started");
        Preconditions.checkNotNull(streamName);

        final String readerGroup = "testReaderGroup" + this.scope + streamName;

        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(this.scope, getClientConfig())) {
            readerGroupManager.createReaderGroup(
                    readerGroup,
                    ReaderGroupConfig.builder().stream(Stream.of(this.scope, streamName)).build());
        }

        final String readerGroupId = UUID.randomUUID().toString();
        return eventStreamClientFactory.createReader(
                readerGroupId,
                readerGroup,
                new IntegerSerializer(),
                ReaderConfig.builder().build());
    }

    /**
     * A gateway interface to Pravega for integration test purposes.
     */
    private interface PravegaGateway {
        /**
         * Starts the gateway.
         */
        void start() throws Exception;

        /**
         * Stops the gateway.
         */
        void stop() throws Exception;

        /**
         * Gets the client configuration with which to connect to the controller.
         */
        ClientConfig getClientConfig();
    }

    class InProcPravegaGateway implements PravegaGateway {

        // The pravega cluster.
        private InProcPravegaCluster inProcPravegaCluster = null;

        @Override
        public void start() throws Exception {
            int zkPort = TestUtils.getAvailableListenPort();
            int controllerPort = TestUtils.getAvailableListenPort();
            int hostPort = TestUtils.getAvailableListenPort();
            int restPort = TestUtils.getAvailableListenPort();

            this.inProcPravegaCluster = InProcPravegaCluster.builder()
                    .isInProcZK(true)
                    .secureZK(true) //configure ZK for security
                    .zkUrl("localhost:" + zkPort)
                    .zkPort(zkPort)
                    .isInMemStorage(true)
                    .isInProcController(true)
                    .controllerCount(1)
                    .restServerPort(restPort)
                    .enableRestServer(enableRestServer)
                    .isInProcSegmentStore(true)
                    .segmentStoreCount(1)
                    .containerCount(4)
                    .enableMetrics(false)
                    .enableAuth(enableAuth)
                    .enableTls(enableTls)
                    .certFile(getPathFromResource(CERT_FILE))
                    .keyFile(getPathFromResource(KEY_FILE))
                    .jksKeyFile(getPathFromResource(STANDALONE_KEYSTORE_FILE))
                    .jksTrustFile(getPathFromResource(STANDALONE_TRUSTSTORE_FILE))
                    .keyPasswordFile(getPathFromResource(STANDALONE_KEYSTORE_PASSWD_FILE))
                    .passwdFile(getPathFromResource(PASSWD_FILE))
                    .userName(PRAVEGA_USERNAME)
                    .passwd(PRAVEGA_PASSWORD)
                    .build();
            this.inProcPravegaCluster.setControllerPorts(new int[]{controllerPort});
            this.inProcPravegaCluster.setSegmentStorePorts(new int[]{hostPort});
            this.inProcPravegaCluster.start();
            LOG.info("Initialized Pravega Cluster");
            LOG.info("Controller port is {}", controllerPort);
            LOG.info("Host port is {}", hostPort);
            LOG.info("REST server port is {}", restPort);
        }

        @Override
        public void stop() throws Exception {
            inProcPravegaCluster.close();
        }

        @Override
        public ClientConfig getClientConfig() {
            return ClientConfig.builder()
                    .controllerURI(URI.create(inProcPravegaCluster.getControllerURI()))
                    .credentials(new DefaultCredentials(PRAVEGA_PASSWORD, PRAVEGA_USERNAME))
                    .validateHostName(enableHostNameValidation)
                    .trustStore(getPathFromResource(CLIENT_TRUST_STORE_FILE))
                    .build();
        }
    }

    class ExternalPravegaGateway implements PravegaGateway {

        private final URI controllerUri;

        public ExternalPravegaGateway(URI controllerUri) {
            this.controllerUri = controllerUri;
        }

        @Override
        public void start() throws Exception {
        }

        @Override
        public void stop() throws Exception {
        }

        @Override
        public ClientConfig getClientConfig() {
            return ClientConfig.builder()
                    .controllerURI(controllerUri)
                    .credentials(new DefaultCredentials(PRAVEGA_PASSWORD, PRAVEGA_USERNAME))
                    .validateHostName(enableHostNameValidation)
                    .trustStore(getPathFromResource(CLIENT_TRUST_STORE_FILE))
                    .build();
        }
    }
}
