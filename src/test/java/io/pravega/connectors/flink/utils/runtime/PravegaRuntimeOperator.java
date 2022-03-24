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
import io.pravega.connectors.flink.utils.IntegerSerializer;
import io.pravega.shared.security.auth.DefaultCredentials;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;

/**
 * A Pravega cluster operator is used for operating Pravega instance.
 */
public class PravegaRuntimeOperator implements Serializable, Closeable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PravegaRuntimeOperator.class);
    private static final String PRAVEGA_USERNAME = "admin";
    private static final String PRAVEGA_PASSWORD = "1111_aaaa";

    private URI controllerUri;

    private String scope;

    private boolean enableHostNameValidation = false;

    private String pravegaClientTrustStore;

    private PravegaConfig pravegaConfig;

    private transient EventStreamClientFactory eventStreamClientFactory;

    public PravegaRuntimeOperator(String scope, String controllerUri, String clientTrustStorePath) {
        this.scope = scope;
        this.controllerUri = URI.create(controllerUri);
        this.pravegaClientTrustStore = clientTrustStorePath;
        this.pravegaConfig = PravegaConfig.fromDefaults()
                .withControllerURI(this.controllerUri)
                .withDefaultScope(this.scope)
                .withCredentials(new DefaultCredentials(PRAVEGA_PASSWORD, PRAVEGA_USERNAME))
                .withHostnameValidation(this.enableHostNameValidation)
                .withTrustStore(this.pravegaClientTrustStore);
    }

    public void initialize() {
        this.eventStreamClientFactory = EventStreamClientFactory.withScope(this.scope, getClientConfig());
    }

    public URI getControllerUri() {
        return controllerUri;
    }

    public String getScope() {
        return scope;
    }

    public boolean isEnableHostNameValidation() {
        return enableHostNameValidation;
    }

    public PravegaConfig getPravegaConfig() {
        return pravegaConfig;
    }

    /**
     * Fetch Pravega client trust store.
     */
    public String getPravegaClientTrustStore() {
        return pravegaClientTrustStore;
    }


    public ClientConfig getClientConfig() {
        return this.pravegaConfig.getClientConfig();
    }

    public String getAuthType() {
        return "Basic";
    }

    public String getAuthToken() {
        String decoded = PRAVEGA_USERNAME + ":" + PRAVEGA_PASSWORD;
        return Base64.getEncoder().encodeToString(decoded.getBytes(StandardCharsets.UTF_8));
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

    @Override
    public void close() throws IOException {
        if (eventStreamClientFactory != null) {
            eventStreamClientFactory.close();
        }
    }
}
